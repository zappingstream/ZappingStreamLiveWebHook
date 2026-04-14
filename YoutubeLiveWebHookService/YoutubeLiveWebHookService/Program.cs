using Firebase.Database;
using Firebase.Database.Query;
using Google.Apis.Services;
using Google.Apis.YouTube.v3;
using Microsoft.AspNetCore.Mvc;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using System.Xml.Linq;

var builder = WebApplication.CreateBuilder(args);

// 1. CONFIGURACIONES
string firebaseUrl = builder.Configuration["Firebase:Url"] ?? "https://zappingstreaming-default-rtdb.firebaseio.com/";
string ytApiKey = builder.Configuration["YouTube:ApiKey"] ?? "";
string firebaseSecret = builder.Configuration["Firebase:Secret"] ?? "";

// 2. INYECCIÓN DE DEPENDENCIAS (Para usar en toda la app y en el proceso de fondo)
builder.Services.AddSingleton(new FirebaseClient(firebaseUrl, new FirebaseOptions
{
    AuthTokenAsyncFactory = () => Task.FromResult(firebaseSecret)
}));

builder.Services.AddSingleton(new YouTubeService(new BaseClientService.Initializer()
{
    ApiKey = ytApiKey,
    ApplicationName = "ZappingStreamingWorker"
}));

// Creamos el "Canal" de comunicación (La cola de IDs)
var channel = Channel.CreateUnbounded<VideoEvent>();
builder.Services.AddSingleton(channel.Writer);
builder.Services.AddSingleton(channel.Reader);

// ¡ACÁ METEMOS TU SERVICIO DE FONDO!
builder.Services.AddHostedService<ProcesadorDeVivosBackground>();

var app = builder.Build();

// 3. EL WEBHOOK (Ahora responde a Google al instante)
app.MapMethods("/webhook", new[] { "GET", "POST" }, async (HttpContext context, ChannelWriter<VideoEvent> escritorCola, ILogger<Program> logger) =>
{
    if (context.Request.Method == HttpMethods.Get)
    {
        if (context.Request.Query.TryGetValue("hub.challenge", out var challenge))
        {
            logger.LogInformation("Suscripción verificada por Google.");
            return Results.Content(challenge, "text/plain");
        }
        return Results.BadRequest("Falta el hub.challenge");
    }

    if (context.Request.Method == HttpMethods.Post)
    {
        using var reader = new StreamReader(context.Request.Body);
        var xmlBody = await reader.ReadToEndAsync();

        try
        {
            var xdoc = XDocument.Parse(xmlBody);
            XNamespace yt = "http://www.youtube.com/xml/schemas/2015";

            var videoIdElement = xdoc.Descendants(yt + "videoId").FirstOrDefault();
            var channelIdElement = xdoc.Descendants(yt + "channelId").FirstOrDefault();

            if (videoIdElement != null)
            {
                string videoId = videoIdElement.Value;
                string channelId = channelIdElement?.Value ?? "";

                logger.LogInformation("¡Aviso recibido! ID: {VideoId}. Mandando a la cola de procesamiento...", videoId);

                await escritorCola.WriteAsync(new VideoEvent(videoId, channelId));
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning("Ignorando XML: {Message}", ex.Message);
        }

        // Respuesta instantánea a Google
        return Results.Ok();
    }

    return Results.StatusCode(405);
});

app.Run();

// --- CLASES Y SERVICIOS AUXILIARES ---

// El paquetito de datos que viaja del webhook al proceso de fondo
public record VideoEvent(string VideoId, string ChannelId);

// EL SERVICIO DE FONDO (El "Timer" con Buffer)
public class ProcesadorDeVivosBackground : BackgroundService
{
    private readonly ChannelReader<VideoEvent> _lectorCola;
    private readonly FirebaseClient _firebaseClient;
    private readonly YouTubeService _youtubeService;
    private readonly ILogger<ProcesadorDeVivosBackground> _logger;

    public ProcesadorDeVivosBackground(
        ChannelReader<VideoEvent> lectorCola,
        FirebaseClient firebaseClient,
        YouTubeService youtubeService,
        ILogger<ProcesadorDeVivosBackground> logger)
    {
        _lectorCola = lectorCola;
        _firebaseClient = firebaseClient;
        _youtubeService = youtubeService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var buffer = new List<VideoEvent>();

        // Este bucle corre para siempre mientras Render esté prendido
        while (!stoppingToken.IsCancellationRequested)
        {
            // 1. El hilo duerme acá sin gastar CPU hasta que entre el PRIMER aviso
            if (await _lectorCola.WaitToReadAsync(stoppingToken))
            {
                // 2. ¡Entró uno! Abrimos la "ventana de recolección" por 1 minuto.
                await Task.Delay(60000, stoppingToken);

                // 3. Se cerró la ventana. Sacamos todo lo que se haya juntado (hasta 50)
                while (buffer.Count < 50 && _lectorCola.TryRead(out var videoEvent))
                {
                    // Filtramos duplicados en el mismo paquete
                    if (!buffer.Any(v => v.VideoId == videoEvent.VideoId))
                    {
                        buffer.Add(videoEvent);
                    }
                }

                if (buffer.Any())
                {
                    _logger.LogInformation("Procesando {Cantidad} webhooks agrupados en el último minuto.", buffer.Count);

                    // 4. Los 10 segundos de cortesía para que YouTube actualice sus servidores
                    await Task.Delay(30000, stoppingToken);

                    // 5. Procesamos todos juntos gastando 1 solo punto
                    await ProcesarBatchAsync(buffer);

                    // 6. Limpiamos el taper para la próxima
                    buffer.Clear();
                }
            }
        }
    }

    private async Task ProcesarBatchAsync(List<VideoEvent> batch)
    {
        try
        {
            // Juntamos todos los IDs separados por coma (ej: "vid1,vid2,vid3")
            string idsJuntos = string.Join(",", batch.Select(v => v.VideoId));

            var videoRequest = _youtubeService.Videos.List("snippet,contentDetails,liveStreamingDetails");
            videoRequest.Id = idsJuntos;
            var videoResponse = await videoRequest.ExecuteAsync();

            var videosEncontrados = videoResponse.Items ?? new List<Google.Apis.YouTube.v3.Data.Video>();

            // Procesamos cada uno y mandamos a Firebase
            foreach (var evento in batch)
            {
                //  Un try-catch ADENTRO del foreach
                try
                {
                    var videoInfo = videosEncontrados.FirstOrDefault(v => v.Id == evento.VideoId);
                    await ActualizarFirebaseParaVideoAsync(evento.VideoId, evento.ChannelId, videoInfo);
                }
                catch (Exception ex)
                {
                    // Si falla un canal, logueamos, pero NO cortamos el foreach.
                    _logger.LogError(ex, "Error aislando el canal {ChannelId} en el batch.", evento.ChannelId);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error grave procesando la lista completa de YouTube");
        }
    }

    private async Task ActualizarFirebaseParaVideoAsync(string videoId, string channelIdInfo, Google.Apis.YouTube.v3.Data.Video videoInfo)
    {
        // 1. DETERMINAR EL ESTADO DEL VIDEO
        string broadcastStatus = videoInfo?.Snippet?.LiveBroadcastContent ?? "none";
        string duracion = videoInfo?.ContentDetails?.Duration ?? "";

        bool esEnVivo = broadcastStatus == "live";
        bool esUpcoming = broadcastStatus == "upcoming";

        // Un video pregrabado (estreno) suele tener duración. Un directo real puro suele tener P0D, PT0S o no tener duración.
        bool esEstreno = videoInfo?.LiveStreamingDetails != null && videoInfo?.LiveStreamingDetails?.ConcurrentViewers == null;

        bool esVivoReal = esEnVivo && !esEstreno;
        bool esUpcomingReal = esUpcoming && !esEstreno;

        // Obtener imagen del directo/miniatura
        string liveImageUrl = videoInfo?.Snippet?.Thumbnails?.High?.Url ?? videoInfo?.Snippet?.Thumbnails?.Medium?.Url ?? "";

        // 2. IDENTIFICAR EL CANAL (Firebase Key)
        string channelName = "";
        string firebaseKey = "";

        if (videoInfo != null)
        {
            channelName = videoInfo.Snippet.ChannelTitle;
            firebaseKey = SanitizarKeyFirebase(channelName);
        }
        else
        {
            // Si el video desapareció, buscamos a quién le pertenecía en Firebase (ya sea en vivos o en upcoming)
            var canalesEnFirebaseBuscador = await _firebaseClient.Child("Channels").OnceAsync<FirebaseChannel>();
            var canalAfectado = canalesEnFirebaseBuscador.FirstOrDefault(c =>
                c.Object.LiveVideoId == videoId); // Buscamos si estaba en vivo

            if (canalAfectado != null)
            {
                firebaseKey = canalAfectado.Key;
                channelName = canalAfectado.Object.ChannelName ?? firebaseKey;
            }
            else
            {
                _logger.LogWarning("Webhook inútil: El video {VideoId} no existe en YT ni está como Vivo en Firebase.", videoId);
                // NOTA: Si el video era un "Upcoming" que fue cancelado antes de emitirse, lo dejaremos pasar por aquí. 
                // Para ser 100% pulcros, podrías buscar también en las carpetas Upcoming, pero no es crítico.
                return;
            }
        }

        // 3. LEER EL ESTADO ACTUAL DEL CANAL EN FIREBASE
        var canalEnFirebase = await _firebaseClient
            .Child("Channels")
            .Child(firebaseKey)
            .OnceSingleAsync<FirebaseChannel>();

        bool estabaEnVivo = canalEnFirebase?.ChannelLive ?? false;
        string videoVivoActualId = canalEnFirebase?.LiveVideoId ?? "";

        // 4. ACTUALIZAR EL NODO PRINCIPAL DEL CANAL (VIVOS)
        object actualizacionParcial;
        if (esVivoReal)
        {
            actualizacionParcial = new
            {
                ChannelLive = true,
                ChannelImgLiveUrl = liveImageUrl,
                LiveVideoId = videoId,
                LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
            };
            _logger.LogInformation("Canal {ChannelName} ON vía Webhook.", channelName);
        }
        else
        {
            if (estabaEnVivo && videoVivoActualId != videoId && !string.IsNullOrEmpty(videoVivoActualId))
            {
                // Un aviso secundario (ej. un video viejo fue borrado, pero el canal sigue en vivo con otro video)
                actualizacionParcial = new { LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") };
                _logger.LogInformation("Aviso secundario. Canal {ChannelName} sigue en vivo con otro ID.", channelName);
            }
            else
            {
                // El canal se apaga
                actualizacionParcial = new
                {
                    ChannelLive = false,
                    ChannelImgLiveUrl = "",
                    LiveVideoId = "",
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
                };
                _logger.LogInformation("Canal {ChannelName} OFF (O es upcoming/estreno) vía Webhook.", channelName);
            }
        }

        // 5. ¡AHORA SÍ! GUARDAMOS EL ESTADO PRINCIPAL EN FIREBASE SIEMPRE
        await _firebaseClient.Child("Channels").Child(firebaseKey).PatchAsync(actualizacionParcial);

        // 6. GESTIONAR LA SUBCARPETA "UPCOMING" (Independiente del nodo principal)
        var upcomingRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Upcoming").Child(videoId);

        if (esUpcomingReal)
        {
            // Si está programado, lo guardamos/actualizamos en la lista
            string horaProgramada = videoInfo?.LiveStreamingDetails?.ScheduledStartTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ") ?? "";

            var upcomingData = new
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? "Directo Programado",
                ScheduledStartTime = horaProgramada,
                ThumbnailUrl = liveImageUrl,
                AddedAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
            };

            await upcomingRef.PutAsync(upcomingData);
            _logger.LogInformation("PROGRAMADO: El canal {ChannelName} tiene un directo upcoming ({VideoId}).", channelName, videoId);
        }
        else
        {
            // Si está EN VIVO, si terminó, si fue borrado, o es un estreno:
            // DEBE DESAPARECER de la lista de próximos. 
            await upcomingRef.DeleteAsync();

            if (esVivoReal)
            {
                _logger.LogInformation("MUDANZA: El video {VideoId} de {ChannelName} pasó a estar EN VIVO.", videoId, channelName);
            }
        }

    }

    private string SanitizarKeyFirebase(string key)
    {
        if (string.IsNullOrWhiteSpace(key)) return "UnknownChannel";

        // 1. Limpiamos los caracteres prohibidos por Firebase
        string keyLimpia = Regex.Replace(key, @"[.#$\[\]]", "").Trim();

        // 2. Codificamos la URL para que los espacios se vuelvan "%20" y el HttpClient no tire EOF
        return Uri.EscapeDataString(keyLimpia);
    }
}

public class FirebaseChannel
{
    public string ChannelName { get; set; }
    public string ChannelDescription { get; set; }
    public string ChannelCity { get; set; }
    public string ChannelType { get; set; }
    public string ChannelLiveUrl { get; set; }
    public string ChannelImgUrl { get; set; }
    public string ChannelImgLiveUrl { get; set; }
    public bool ChannelLive { get; set; }
    public string LastActivityAt { get; set; }
    public string LiveVideoId { get; set; }
}
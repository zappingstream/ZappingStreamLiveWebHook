using Firebase.Database;
using Firebase.Database.Query;
using Google.Apis.Services;
using Google.Apis.YouTube.v3;
using Microsoft.AspNetCore.Mvc;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using System.Threading.Channels;

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
                    _logger.LogInformation("Procesando {Cantidad} webhooks agrupados en el último segundo.", buffer.Count);

                    // 4. Los 10 segundos de cortesía para que YouTube actualice sus servidores
                    await Task.Delay(10000, stoppingToken);

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

            // ¡UNA SOLA CONSULTA A LA API!
            var videoRequest = _youtubeService.Videos.List("snippet,contentDetails");
            videoRequest.Id = idsJuntos;
            var videoResponse = await videoRequest.ExecuteAsync();

            var videosEncontrados = videoResponse.Items ?? new List<Google.Apis.YouTube.v3.Data.Video>();

            // Procesamos cada uno y mandamos a Firebase
            foreach (var evento in batch)
            {
                var videoInfo = videosEncontrados.FirstOrDefault(v => v.Id == evento.VideoId);
                await ActualizarFirebaseParaVideoAsync(evento.VideoId, evento.ChannelId, videoInfo);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error grave procesando el batch de videos");
        }
    }

    private async Task ActualizarFirebaseParaVideoAsync(string videoId, string channelIdInfo, Google.Apis.YouTube.v3.Data.Video videoInfo)
    {
        // Si videoInfo es null, significa que el video fue borrado o es privado
        bool estaEnVivo = videoInfo?.Snippet?.LiveBroadcastContent == "live";
        string duracion = videoInfo?.ContentDetails?.Duration ?? "";

        bool esEstreno = estaEnVivo && duracion != "P0D" && duracion != "PT0S";
        bool esVivoReal = estaEnVivo && !esEstreno;

        string liveImageUrl = esVivoReal ?
            (videoInfo.Snippet.Thumbnails?.High?.Url ?? videoInfo.Snippet.Thumbnails?.Medium?.Url ?? "") : "";

        // Si no tenemos info de YouTube, usamos el ChannelId que nos mandó el Webhook
        string channelName = videoInfo?.Snippet?.ChannelTitle ?? channelIdInfo;
        string firebaseKey = SanitizarKeyFirebase(channelName);

        // LEER EL ESTADO ACTUAL EN FIREBASE
        var canalEnFirebase = await _firebaseClient
            .Child("Channels")
            .Child(firebaseKey)
            .OnceSingleAsync<FirebaseChannel>();

        bool estabaEnVivo = canalEnFirebase?.ChannelLive ?? false;
        string videoVivoActualId = canalEnFirebase?.LiveVideoId ?? "";

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
                actualizacionParcial = new { LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") };
                _logger.LogInformation("Aviso secundario. Canal {ChannelName} sigue en vivo con {VideoVivoActualId}.", channelName, videoVivoActualId);
            }
            else
            {
                actualizacionParcial = new
                {
                    ChannelLive = false,
                    ChannelImgLiveUrl = "",
                    LiveVideoId = "",
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
                };
                _logger.LogInformation("Canal {ChannelName} OFF vía Webhook.", channelName);
            }
        }

        // MANDAMOS A FIREBASE
        await _firebaseClient
            .Child("Channels")
            .Child(firebaseKey)
            .PatchAsync(actualizacionParcial);
    }

    private string SanitizarKeyFirebase(string key)
    {
        if (string.IsNullOrWhiteSpace(key)) return "UnknownChannel";
        return Regex.Replace(key, @"[.#$\[\]]", "").Trim();
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
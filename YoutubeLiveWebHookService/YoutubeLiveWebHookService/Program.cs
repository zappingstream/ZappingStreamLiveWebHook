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

// 2. INYECCIÓN DE DEPENDENCIAS
builder.Services.AddSingleton(new FirebaseClient(firebaseUrl, new FirebaseOptions
{
    AuthTokenAsyncFactory = () => Task.FromResult(firebaseSecret)
}));

builder.Services.AddSingleton(new YouTubeService(new BaseClientService.Initializer()
{
    ApiKey = ytApiKey,
    ApplicationName = "ZappingStreamingWorker"
}));

var channel = Channel.CreateUnbounded<VideoEvent>();
builder.Services.AddSingleton(channel.Writer);
builder.Services.AddSingleton(channel.Reader);

builder.Services.AddHostedService<ProcesadorDeVivosBackground>();

var app = builder.Build();

// 3. EL WEBHOOK
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

        return Results.Ok();
    }

    return Results.StatusCode(405);
});

app.Run();

// --- CLASES Y SERVICIOS AUXILIARES ---

public record VideoEvent(string VideoId, string ChannelId);

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

        while (!stoppingToken.IsCancellationRequested)
        {
            if (await _lectorCola.WaitToReadAsync(stoppingToken))
            {
                await Task.Delay(60000, stoppingToken);

                while (buffer.Count < 50 && _lectorCola.TryRead(out var videoEvent))
                {
                    if (!buffer.Any(v => v.VideoId == videoEvent.VideoId))
                    {
                        buffer.Add(videoEvent);
                    }
                }

                if (buffer.Any())
                {
                    _logger.LogInformation("Procesando {Cantidad} webhooks agrupados en el último minuto.", buffer.Count);
                    await Task.Delay(30000, stoppingToken);
                    await ProcesarBatchAsync(buffer);
                    buffer.Clear();
                }
            }
        }
    }

    private async Task ProcesarBatchAsync(List<VideoEvent> batch)
    {
        try
        {
            string idsJuntos = string.Join(",", batch.Select(v => v.VideoId));

            var videoRequest = _youtubeService.Videos.List("snippet,contentDetails,liveStreamingDetails");
            videoRequest.Id = idsJuntos;
            var videoResponse = await videoRequest.ExecuteAsync();

            var videosEncontrados = videoResponse.Items ?? new List<Google.Apis.YouTube.v3.Data.Video>();

            foreach (var evento in batch)
            {
                try
                {
                    var videoInfo = videosEncontrados.FirstOrDefault(v => v.Id == evento.VideoId);
                    await ActualizarFirebaseParaVideoAsync(evento.VideoId, evento.ChannelId, videoInfo);
                }
                catch (Exception ex)
                {
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

        bool esEnVivo = broadcastStatus == "live";
        bool esUpcoming = broadcastStatus == "upcoming";

        bool esEstreno = videoInfo?.LiveStreamingDetails != null && videoInfo?.LiveStreamingDetails?.ConcurrentViewers == null &&
                         videoInfo?.ContentDetails != null && !(videoInfo?.ContentDetails?.Duration == "P0D" || videoInfo?.ContentDetails?.Duration == "PT0D");

        bool esVivoReal = esEnVivo && !esEstreno;
        bool esUpcomingReal = esUpcoming && !esEstreno;

        string liveImageUrl = videoInfo?.Snippet?.Thumbnails?.High?.Url ?? videoInfo?.Snippet?.Thumbnails?.Medium?.Url ?? "";

        // 2. IDENTIFICAR EL CANAL
        string channelName = "";
        string firebaseKey = "";

        if (videoInfo != null)
        {
            channelName = videoInfo.Snippet.ChannelTitle;
            firebaseKey = SanitizarKeyFirebase(channelName);
        }
        else
        {
            // Búsqueda profunda si el video desapareció (Buscamos en las listas nuevas)
            var canalesEnFirebaseBuscador = await _firebaseClient.Child("Channels").OnceAsync<FirebaseChannel>();
            var canalAfectado = canalesEnFirebaseBuscador.FirstOrDefault(c =>
                c.Object.LiveVideoId == videoId ||
                (c.Object.Actives != null && c.Object.Actives.ContainsKey(videoId)) ||
                (c.Object.Upcoming != null && c.Object.Upcoming.ContainsKey(videoId)));

            if (canalAfectado != null)
            {
                firebaseKey = canalAfectado.Key;
                channelName = canalAfectado.Object.ChannelName ?? firebaseKey;
            }
            else
            {
                _logger.LogWarning("Webhook inútil: El video {VideoId} no existe en YT ni está registrado en Firebase.", videoId);
                return;
            }
        }

        // 3. LEER EL ESTADO ACTUAL DEL CANAL EN FIREBASE
        var canalEnFirebase = await _firebaseClient.Child("Channels").Child(firebaseKey).OnceSingleAsync<FirebaseChannel>();
        var vivosActuales = canalEnFirebase?.Actives ?? new Dictionary<string, ActiveVideo>();

        // Referencias directas a las subcarpetas del video
        var activeRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Actives").Child(videoId);
        var upcomingRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Upcoming").Child(videoId);

        object actualizacionParcial;

        // 4. LOGICA DE VIVOS MÚLTIPLES
        if (esVivoReal)
        {
            // Agregamos a la colección Actives
            var activeData = new ActiveVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? "Directo",
                AddedAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                ThumbnailUrl = liveImageUrl
            };
            await activeRef.PutAsync(activeData);

            // Mantenemos el parche Legacy para no romper el front viejo
            actualizacionParcial = new
            {
                ChannelLive = true,
                ChannelImgLiveUrl = liveImageUrl,
                LiveVideoId = videoId, // Pasa a ser "el último vivo detectado"
                LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
            };
            _logger.LogInformation("Canal {ChannelName} ON vía Webhook. Video {VideoId} agregado a Actives.", channelName, videoId);
        }
        else
        {
            // El video se apagó o cambió de estado. Lo volamos de la lista de Activos.
            await activeRef.DeleteAsync();

            // Verificamos si quedaban OTROS videos en vivo (excluyendo el que se acaba de apagar)
            var vivosRestantes = vivosActuales.Where(kv => kv.Key != videoId).Select(kv => kv.Value).ToList();
            bool quedanOtrosVivos = vivosRestantes.Any();

            if (quedanOtrosVivos)
            {
                // Agarramos el stream sobreviviente más reciente para hacer fallback
                var fallbackVideo = vivosRestantes.OrderByDescending(v => v.AddedAt).First();

                // Actualizamos la última actividad y hacemos que el front salte al otro stream
                actualizacionParcial = new
                {
                    LiveVideoId = fallbackVideo.VideoId,
                    ChannelImgLiveUrl = fallbackVideo.ThumbnailUrl,
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
                };
                _logger.LogInformation("Aviso secundario. Se apagó {VideoId} pero el canal {ChannelName} hace fallback automático al stream {FallbackId}.", videoId, channelName, fallbackVideo.VideoId);
            }
            else
            {
                // Se apagó el ÚLTIMO vivo que quedaba, ahora sí apagamos el canal entero
                actualizacionParcial = new
                {
                    ChannelLive = false,
                    ChannelImgLiveUrl = "",
                    LiveVideoId = "",
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
                };
                _logger.LogInformation("Canal {ChannelName} OFF totalmente vía Webhook.", channelName);
            }
        }

        // Aplicamos el parche en el nodo principal (Legacy + LastActivity)
        await _firebaseClient.Child("Channels").Child(firebaseKey).PatchAsync(actualizacionParcial);

        // 5. GESTIONAR LA SUBCARPETA "UPCOMING"
        if (esUpcomingReal)
        {
            string horaProgramada = videoInfo?.LiveStreamingDetails?.ScheduledStartTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ") ?? "";
            var upcomingData = new UpcomingVideo
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
            // Si está EN VIVO, o si terminó/fue borrado, se borra de Upcoming
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
        string keyLimpia = Regex.Replace(key, @"[.#$\[\]]", "").Trim();
        return Uri.EscapeDataString(keyLimpia);
    }
}

// --- MODELOS DE DATOS ---
public class FirebaseChannel
{
    public string ChannelName { get; set; }
    public string ChannelDescription { get; set; }
    public string ChannelCity { get; set; }
    public string ChannelType { get; set; }
    public string ChannelLiveUrl { get; set; }
    public string ChannelImgUrl { get; set; }

    // Legacy
    public string ChannelImgLiveUrl { get; set; }
    public bool ChannelLive { get; set; }
    public string LiveVideoId { get; set; }
    public string LastActivityAt { get; set; }

    // Colecciones multi-estado
    public Dictionary<string, UpcomingVideo> Upcoming { get; set; }
    public Dictionary<string, ActiveVideo> Actives { get; set; }
}

public class UpcomingVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ScheduledStartTime { get; set; }
    public string ThumbnailUrl { get; set; }
    public string AddedAt { get; set; }
}

public class ActiveVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ScheduledStartTime { get; set; }
    public string ThumbnailUrl { get; set; }
    public string AddedAt { get; set; }
}
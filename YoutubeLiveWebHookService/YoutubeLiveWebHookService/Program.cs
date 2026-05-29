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
        // EXTRACCIÓN DE TODOS LOS TIEMPOS DISPONIBLES (Formato ISO 8601 UTC)
        string publishedAt = videoInfo?.Snippet?.PublishedAtDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string scheduledStart = videoInfo?.LiveStreamingDetails?.ScheduledStartTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string actualStart = videoInfo?.LiveStreamingDetails?.ActualStartTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string actualEnd = videoInfo?.LiveStreamingDetails?.ActualEndTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string sysTimeNow = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

        // 1. DETERMINAR EL ESTADO DEL VIDEO
        string broadcastStatus = videoInfo?.Snippet?.LiveBroadcastContent ?? "none";

        bool esEnVivo = broadcastStatus == "live";
        bool esUpcoming = broadcastStatus == "upcoming";

        bool tieneDuracion = videoInfo?.ContentDetails != null &&
                             videoInfo.ContentDetails.Duration != "P0D" &&
                             videoInfo.ContentDetails.Duration != "PT0D";

        bool esEstreno = (esEnVivo || esUpcoming) && tieneDuracion;
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
        var upcomingActuales = canalEnFirebase?.Upcoming ?? new Dictionary<string, UpcomingVideo>();
        string legacyLiveVideoId = canalEnFirebase?.LiveVideoId ?? "";

        bool estabaEnActivos = vivosActuales.ContainsKey(videoId);
        bool eraElVivoLegacy = legacyLiveVideoId == videoId;
        bool estabaEnUpcoming = upcomingActuales.ContainsKey(videoId);

        if (!esEnVivo && !esUpcoming && !estabaEnActivos && !eraElVivoLegacy && !estabaEnUpcoming)
        {
            _logger.LogInformation("Escudo activado: Registrando actividad por VOD/Reel {VideoId} del canal {ChannelName}.", videoId, channelName);
            var actualizacionActividad = new { LastActivityAt = sysTimeNow };
            await _firebaseClient.Child("Channels").Child(firebaseKey).PatchAsync(actualizacionActividad);
            return;
        }

        // Referencias directas a las subcarpetas del video
        var activeRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Actives").Child(videoId);
        var upcomingRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Upcoming").Child(videoId);
        var pastRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Past").Child(videoId);

        object actualizacionParcial = null;
        bool huboCambiosEnVivos = false;

        // 4. GESTIONAR LA SUBCARPETA "ACTIVES" Y TRANSICIONES A "PAST"
        if (esEnVivo)
        {
            var activeData = new ActiveVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? (esEstreno ? "Estreno en curso" : "Directo"),
                ThumbnailUrl = liveImageUrl,
                IsPremiere = esEstreno,

                // Tiempos
                PublishedAt = publishedAt,
                ScheduledStartTime = scheduledStart,
                ActualStartTime = actualStart ?? sysTimeNow, // Resguardo si YT tarda en mandarlo
                ActualEndTime = actualEnd,
                AddedAt = sysTimeNow
            };

            await activeRef.PutAsync(activeData);

            vivosActuales[videoId] = activeData;
            huboCambiosEnVivos = true;
        }
        else if (estabaEnActivos || eraElVivoLegacy)
        {
            await activeRef.DeleteAsync();
            vivosActuales.TryGetValue(videoId, out var videoActivo);

            // Resguardamos los tiempos viejos por si el API de YT no los trae más
            string fallbackPublished = publishedAt ?? videoActivo?.PublishedAt;
            string fallbackScheduled = scheduledStart ?? videoActivo?.ScheduledStartTime;
            string fallbackActualStart = actualStart ?? videoActivo?.ActualStartTime;

            var pastData = new PastVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? videoActivo?.Title ?? "Directo finalizado",
                ThumbnailUrl = liveImageUrl,
                WasPremiere = videoActivo?.IsPremiere ?? false,

                // Tiempos
                PublishedAt = fallbackPublished,
                ScheduledStartTime = fallbackScheduled,
                ActualStartTime = fallbackActualStart,
                ActualEndTime = actualEnd ?? sysTimeNow,
                EndedAt = sysTimeNow
            };

            await pastRef.PutAsync(pastData);
            _logger.LogInformation("FINALIZADO: El video {VideoId} de {ChannelName} terminó y pasó a Past.", videoId, channelName);

            if (vivosActuales.ContainsKey(videoId))
            {
                vivosActuales.Remove(videoId);
            }
            huboCambiosEnVivos = true;
        }

        if (huboCambiosEnVivos)
        {
            var vivosRestantes = vivosActuales.Values.ToList();

            if (vivosRestantes.Any())
            {
                var streamGanador = vivosRestantes
                    .OrderBy(v => v.IsPremiere)
                    .ThenByDescending(v => v.AddedAt)
                    .First();

                actualizacionParcial = new
                {
                    ChannelLive = true,
                    LiveVideoId = streamGanador.VideoId,
                    ChannelImgLiveUrl = streamGanador.ThumbnailUrl,
                    LastActivityAt = sysTimeNow,
                    IsPremiere = streamGanador.IsPremiere
                };

                _logger.LogInformation("Canal {ChannelName} reevaluado. Stream principal elegido: {GanadorId} (¿Es Estreno?: {EsEstreno})",
                    channelName, streamGanador.VideoId, streamGanador.IsPremiere);
            }
            else
            {
                bool sobreviveLegacy = !string.IsNullOrEmpty(legacyLiveVideoId) && legacyLiveVideoId != videoId && !vivosActuales.ContainsKey(legacyLiveVideoId);

                if (sobreviveLegacy)
                {
                    actualizacionParcial = new
                    {
                        LiveVideoId = legacyLiveVideoId,
                        LastActivityAt = sysTimeNow
                    };
                    _logger.LogInformation("Aviso: El canal {ChannelName} sobrevive por Legacy ID: {LegacyId}", channelName, legacyLiveVideoId);
                }
                else
                {
                    actualizacionParcial = new
                    {
                        ChannelLive = false,
                        ChannelImgLiveUrl = "",
                        LiveVideoId = "",
                        LastActivityAt = sysTimeNow,
                        IsPremiere = false
                    };
                    _logger.LogInformation("Canal {ChannelName} OFF totalmente vía Webhook.", channelName);
                }
            }
        }

        if (actualizacionParcial != null)
        {
            await _firebaseClient.Child("Channels").Child(firebaseKey).PatchAsync(actualizacionParcial);
        }

        // 5. GESTIONAR LA SUBCARPETA "UPCOMING" (INCLUIDOS PREMIERES)
        if (esUpcoming)
        {
            var upcomingData = new UpcomingVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? (esEstreno ? "Estreno Programado" : "Directo Programado"),
                ThumbnailUrl = liveImageUrl,
                IsPremiere = esEstreno,

                // Tiempos
                PublishedAt = publishedAt,
                ScheduledStartTime = scheduledStart,
                ActualStartTime = actualStart,
                ActualEndTime = actualEnd,
                AddedAt = sysTimeNow
            };

            await upcomingRef.PutAsync(upcomingData);
            _logger.LogInformation("PROGRAMADO: {ChannelName} tiene un upcoming ({VideoId}). ¿Es Estreno?: {EsEstreno}", channelName, videoId, esEstreno);
        }
        else if (estabaEnUpcoming)
        {
            await upcomingRef.DeleteAsync();

            if (esEnVivo)
            {
                _logger.LogInformation("MUDANZA: El video {VideoId} de {ChannelName} pasó a estar EN VIVO.", videoId, channelName);
            }
            else
            {
                upcomingActuales.TryGetValue(videoId, out var videoUpcoming);

                string fallbackPublished = publishedAt ?? videoUpcoming?.PublishedAt;
                string fallbackScheduled = scheduledStart ?? videoUpcoming?.ScheduledStartTime;

                var pastData = new PastVideo
                {
                    VideoId = videoId,
                    Title = videoInfo?.Snippet?.Title ?? videoUpcoming?.Title ?? "Programación cancelada",
                    ThumbnailUrl = liveImageUrl,
                    WasPremiere = videoUpcoming?.IsPremiere ?? false,

                    // Tiempos
                    PublishedAt = fallbackPublished,
                    ScheduledStartTime = fallbackScheduled,
                    ActualStartTime = actualStart,
                    ActualEndTime = actualEnd ?? sysTimeNow,
                    EndedAt = sysTimeNow
                };

                await pastRef.PutAsync(pastData);
                _logger.LogInformation("CANCELADO: El video upcoming {VideoId} de {ChannelName} se canceló y pasó a Past.", videoId, channelName);
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
    public bool IsPremiere { get; set; }

    // Colecciones multi-estado
    public Dictionary<string, UpcomingVideo> Upcoming { get; set; }
    public Dictionary<string, ActiveVideo> Actives { get; set; }
    public Dictionary<string, PastVideo> Past { get; set; }
}

public class UpcomingVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ThumbnailUrl { get; set; }
    public bool IsPremiere { get; set; }

    // Tiempos
    public string PublishedAt { get; set; }
    public string ScheduledStartTime { get; set; }
    public string ActualStartTime { get; set; }
    public string ActualEndTime { get; set; }
    public string AddedAt { get; set; }
}

public class ActiveVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ThumbnailUrl { get; set; }
    public bool IsPremiere { get; set; }

    // Tiempos
    public string PublishedAt { get; set; }
    public string ScheduledStartTime { get; set; }
    public string ActualStartTime { get; set; }
    public string ActualEndTime { get; set; }
    public string AddedAt { get; set; }
}

public class PastVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ThumbnailUrl { get; set; }
    public bool WasPremiere { get; set; }

    // Tiempos
    public string PublishedAt { get; set; }
    public string ScheduledStartTime { get; set; }
    public string ActualStartTime { get; set; }
    public string ActualEndTime { get; set; }
    public string EndedAt { get; set; }
}
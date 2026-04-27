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

        // Ya no distinguimos entre "Vivo Real" o "Upcoming Real". 
        // Ahora, si está en 'live', es un stream en curso (sea directo o estreno).
        bool esEnVivo = broadcastStatus == "live";
        bool esUpcoming = broadcastStatus == "upcoming";

        // Verificamos si tiene una duración real (propio de los estrenos/premieres grabados)
        bool tieneDuracion = videoInfo?.ContentDetails != null &&
                             videoInfo.ContentDetails.Duration != "P0D" &&
                             videoInfo.ContentDetails.Duration != "PT0D";

        // Es un estreno SI está live o upcoming, PERO ya tiene duración
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

        // --- EL ESCUDO ANTI-REELS / VODs ---
        bool estabaEnActivos = vivosActuales.ContainsKey(videoId);
        bool eraElVivoLegacy = legacyLiveVideoId == videoId;
        bool estabaEnUpcoming = upcomingActuales.ContainsKey(videoId);

        // Simplificado: Si NO está en vivo, NO es upcoming y NUNCA lo tuvimos registrado, es un VOD clásico.
        if (!esEnVivo && !esUpcoming && !estabaEnActivos && !eraElVivoLegacy && !estabaEnUpcoming)
        {
            _logger.LogInformation("Escudo activado: Registrando actividad por VOD/Reel {VideoId} del canal {ChannelName}.", videoId, channelName);
            var actualizacionActividad = new { LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ") };
            await _firebaseClient.Child("Channels").Child(firebaseKey).PatchAsync(actualizacionActividad);
            return;
        }

        // Referencias directas a las subcarpetas del video
        var activeRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Actives").Child(videoId);
        var upcomingRef = _firebaseClient.Child("Channels").Child(firebaseKey).Child("Upcoming").Child(videoId);

        object actualizacionParcial = null;

        // 4. LÓGICA DE VIVOS MÚLTIPLES Y ESTRENOS LIVE
        if (esEnVivo)
        {
            var activeData = new ActiveVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? (esEstreno ? "Estreno en curso" : "Directo"),
                AddedAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                ThumbnailUrl = liveImageUrl,
                IsPremiere = esEstreno // Guardamos el flag en la subcolección
            };
            await activeRef.PutAsync(activeData);

            actualizacionParcial = new
            {
                ChannelLive = true,
                ChannelImgLiveUrl = liveImageUrl,
                LiveVideoId = videoId,
                LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                IsPremiere = esEstreno // Guardamos el flag en el nodo principal del canal
            };
            _logger.LogInformation("Canal {ChannelName} ON. Video {VideoId} a Actives. ¿Es Estreno?: {EsEstreno}", channelName, videoId, esEstreno);
        }
        else if (estabaEnActivos || eraElVivoLegacy)
        {
            await activeRef.DeleteAsync();

            var vivosRestantes = vivosActuales.Where(kv => kv.Key != videoId).Select(kv => kv.Value).ToList();
            bool quedanOtrosVivos = vivosRestantes.Any();
            bool sobreviveLegacy = !string.IsNullOrEmpty(legacyLiveVideoId) && legacyLiveVideoId != videoId && !vivosActuales.ContainsKey(legacyLiveVideoId);

            if (quedanOtrosVivos || sobreviveLegacy)
            {
                var fallbackVivo = quedanOtrosVivos ? vivosRestantes.OrderByDescending(v => v.AddedAt).First() : null;
                string fallbackId = fallbackVivo?.VideoId ?? legacyLiveVideoId;
                string fallbackImg = fallbackVivo?.ThumbnailUrl ?? canalEnFirebase?.ChannelImgLiveUrl;
                bool fallbackIsPremiere = fallbackVivo?.IsPremiere ?? canalEnFirebase?.IsPremiere ?? false;

                actualizacionParcial = new
                {
                    LiveVideoId = fallbackId,
                    ChannelImgLiveUrl = fallbackImg,
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                    IsPremiere = fallbackIsPremiere // Hereda el estado de premiere del vivo que queda
                };
                _logger.LogInformation("Aviso secundario. Se apagó {VideoId} pero {ChannelName} hace fallback automático al ID {FallbackId}.", videoId, channelName, fallbackId);
            }
            else
            {
                actualizacionParcial = new
                {
                    ChannelLive = false,
                    ChannelImgLiveUrl = "",
                    LiveVideoId = "",
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                    IsPremiere = false // Limpiamos el flag porque el canal ya no está en vivo
                };
                _logger.LogInformation("Canal {ChannelName} OFF totalmente vía Webhook.", channelName);
            }
        }

        if (actualizacionParcial != null)
        {
            await _firebaseClient.Child("Channels").Child(firebaseKey).PatchAsync(actualizacionParcial);
        }

        // 5. GESTIONAR LA SUBCARPETA "UPCOMING" (INCLUIDOS PREMIERES)
        if (esUpcoming)
        {
            string horaProgramada = videoInfo?.LiveStreamingDetails?.ScheduledStartTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ") ?? "";
            var upcomingData = new UpcomingVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? (esEstreno ? "Estreno Programado" : "Directo Programado"),
                ScheduledStartTime = horaProgramada,
                ThumbnailUrl = liveImageUrl,
                AddedAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                IsPremiere = esEstreno // Guardamos el flag en Upcoming
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
    public bool IsPremiere { get; set; } // <-- NUEVO

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
    public bool IsPremiere { get; set; } // <-- NUEVO
}

public class ActiveVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ScheduledStartTime { get; set; }
    public string ThumbnailUrl { get; set; }
    public string AddedAt { get; set; }
    public bool IsPremiere { get; set; } // <-- NUEVO
}
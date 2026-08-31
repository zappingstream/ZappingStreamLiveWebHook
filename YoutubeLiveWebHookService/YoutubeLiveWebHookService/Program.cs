using Google.Apis.Services;
using Google.Apis.YouTube.v3;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using System.Xml.Linq;

var builder = WebApplication.CreateBuilder(args);

// Apagar reloadOnChange para no consumir instancias de inotify en Linux (Render)
builder.Host.ConfigureAppConfiguration((hostingContext, config) =>
{
    foreach (var source in config.Sources.OfType<Microsoft.Extensions.Configuration.Json.JsonConfigurationSource>())
    {
        source.ReloadOnChange = false;
    }
});

// 1. CONFIGURACIONES
string mongoUri = builder.Configuration["MongoDB:ConnectionString"] ?? "mongodb+srv://...";
string dbName = builder.Configuration["MongoDB:DatabaseName"] ?? "ZappingStreaming";
string ytApiKey = builder.Configuration["YouTube:ApiKey"] ?? "";

// 2. INYECCIÓN DE DEPENDENCIAS
var mongoClient = new MongoClient(mongoUri);
var database = mongoClient.GetDatabase(dbName);
builder.Services.AddSingleton(database);

builder.Services.AddSingleton(new YouTubeService(new BaseClientService.Initializer()
{
    ApiKey = ytApiKey,
    ApplicationName = "ZappingStreamingWebhook"
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

            var videoIdElements = xdoc.Descendants(yt + "videoId").ToList();
            var channelIdElement = xdoc.Descendants(yt + "channelId").FirstOrDefault();
            string channelId = channelIdElement?.Value ?? "";

            // 1. Encolamos el video exacto que nos manda YouTube en esta notificación
            foreach (var videoIdElement in videoIdElements)
            {
                string videoId = videoIdElement.Value;
                logger.LogInformation("¡Aviso directo recibido! ID: {VideoId}. Mandando a la cola...", videoId);
                await escritorCola.WriteAsync(new VideoEvent(videoId, channelId));
            }

            // 2. Le avisamos a YouTube inmediatamente que recibimos el aviso
            // Y en segundo plano (para no bloquear) descargamos el feed RSS completo del canal como respaldo
            if (!string.IsNullOrEmpty(channelId))
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        logger.LogInformation("Webhook de {ChannelId}. Revisando feed RSS completo por si perdimos vivos hoy...", channelId);
                        using var httpClient = new HttpClient();
                        var feedXml = await httpClient.GetStringAsync($"https://www.youtube.com/feeds/videos.xml?channel_id={channelId}");
                        var feedDoc = XDocument.Parse(feedXml);
                        XNamespace atom = "http://www.w3.org/2005/Atom";
                        
                        var today = DateTime.UtcNow.Date;
                        var entries = feedDoc.Descendants(atom + "entry");

                        foreach (var entry in entries)
                        {
                            var publishedStr = entry.Element(atom + "published")?.Value;
                            if (DateTime.TryParse(publishedStr, out var publishedDate) && publishedDate.ToUniversalTime().Date == today)
                            {
                                var feedVideoId = entry.Element(yt + "videoId")?.Value;
                                if (!string.IsNullOrEmpty(feedVideoId))
                                {
                                    logger.LogInformation("Encontrado video de hoy en RSS: {VideoId}. Mandando a la cola...", feedVideoId);
                                    await escritorCola.WriteAsync(new VideoEvent(feedVideoId, channelId));
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "Error al leer feed RSS de {ChannelId}", channelId);
                    }
                });
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
    private readonly IMongoCollection<ZappingChannel> _channelsCollection;
    private readonly YouTubeService _youtubeService;
    private readonly ILogger<ProcesadorDeVivosBackground> _logger;

    public ProcesadorDeVivosBackground(
        ChannelReader<VideoEvent> lectorCola,
        IMongoDatabase database,
        YouTubeService youtubeService,
        ILogger<ProcesadorDeVivosBackground> logger)
    {
        _lectorCola = lectorCola;
        _channelsCollection = database.GetCollection<ZappingChannel>("channels");
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
                await Task.Delay(60000, stoppingToken); // Espera de 1 min para agrupar

                while (buffer.Count < 50 && _lectorCola.TryRead(out var videoEvent))
                {
                    if (!buffer.Any(v => v.VideoId == videoEvent.VideoId))
                    {
                        buffer.Add(videoEvent);
                    }
                }

                if (buffer.Any())
                {
                    _logger.LogInformation("Procesando {Cantidad} webhooks agrupados.", buffer.Count);
                    await Task.Delay(30000, stoppingToken); // Gap adicional para dejar que YT propague la metadata
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
                    await ActualizarMongoParaVideoAsync(evento.VideoId, evento.ChannelId, videoInfo);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error procesando el video {VideoId} del canal {ChannelId}.", evento.VideoId, evento.ChannelId);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error grave procesando el batch de YouTube.");
        }
    }

    private async Task ActualizarMongoParaVideoAsync(string videoId, string channelIdInfo, Google.Apis.YouTube.v3.Data.Video videoInfo)
    {
        string sysTimeNow = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

        // 1. EXTRACCIÓN DE DATOS DE YOUTUBE
        string publishedAt = videoInfo?.Snippet?.PublishedAtDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string scheduledStart = videoInfo?.LiveStreamingDetails?.ScheduledStartTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string actualStart = videoInfo?.LiveStreamingDetails?.ActualStartTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");
        string actualEnd = videoInfo?.LiveStreamingDetails?.ActualEndTimeDateTimeOffset?.ToString("yyyy-MM-ddTHH:mm:ssZ");

        string broadcastStatus = videoInfo?.Snippet?.LiveBroadcastContent ?? "none";
        bool esEnVivo = broadcastStatus == "live";
        bool esUpcoming = broadcastStatus == "upcoming";
        bool tieneDuracion = videoInfo?.ContentDetails != null &&
                             videoInfo.ContentDetails.Duration != "P0D" &&
                             videoInfo.ContentDetails.Duration != "PT0D";
        bool esEstreno = (esEnVivo || esUpcoming) && tieneDuracion;
        string liveImageUrl = videoInfo?.Snippet?.Thumbnails?.High?.Url ?? videoInfo?.Snippet?.Thumbnails?.Medium?.Url ?? "";

        // 2. RECUPERAR EL CANAL DE MONGO
        // Ahora usamos el ID de YouTube (UC...) directamente
        string targetChannelId = videoInfo?.Snippet?.ChannelId ?? channelIdInfo;

        ZappingChannel canal;

        if (!string.IsNullOrEmpty(targetChannelId))
        {
            canal = await _channelsCollection.Find(c => c.Id == targetChannelId).FirstOrDefaultAsync();
        }
        else
        {
            // Fallback limpio: buscar el video solo en los diccionarios
            canal = await _channelsCollection.Find(c =>
                c.Actives.ContainsKey(videoId) ||
                c.Upcoming.ContainsKey(videoId) ||
                c.Past.ContainsKey(videoId)
            ).FirstOrDefaultAsync();
        }

        if (canal == null)
        {
            _logger.LogWarning("Webhook ignorado: El canal {ChannelId} no está registrado en la base de datos.", targetChannelId);
            return;
        }

        // Inicializar diccionarios si vienen null de la BD
        canal.Actives ??= new Dictionary<string, ActiveVideo>();
        canal.Upcoming ??= new Dictionary<string, UpcomingVideo>();
        canal.Past ??= new Dictionary<string, PastVideo>();

        bool estabaEnActivos = canal.Actives.ContainsKey(videoId);
        bool estabaEnUpcoming = canal.Upcoming.ContainsKey(videoId);
        bool estabaEnPast = canal.Past.ContainsKey(videoId);

        // ESCUDO: Actividad por VOD/Reel
        if (!esEnVivo && !esUpcoming && !estabaEnActivos && !estabaEnUpcoming && !estabaEnPast)
        {
            _logger.LogInformation("Escudo activado: VOD/Reel detectado en {ChannelName}. Solo actualizo actividad.", canal.ChannelName);
            var update = Builders<ZappingChannel>.Update.Set(c => c.LastActivityAt, sysTimeNow);
            await _channelsCollection.UpdateOneAsync(c => c.Id == canal.Id, update);
            return;
        }

        bool huboCambiosEnVivos = false;

        // 3. GESTIONAR "ACTIVES" Y TRANSICIONES A "PAST"
        if (esEnVivo)
        {
            // 1. Verificamos si el video ya existía en nuestros registros activos
            canal.Actives.TryGetValue(videoId, out var videoPrevio);

            // 2. Blindamos el valor: si ya existía, mantenemos lo que teníamos. Si es nuevo, usamos la lógica de YouTube.
            bool esRealmenteEstreno = videoPrevio?.IsPremiere ?? esEstreno;

            canal.Actives[videoId] = new ActiveVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? (esEstreno ? "Estreno en curso" : "Directo"),
                ThumbnailUrl = liveImageUrl,
                IsPremiere = esRealmenteEstreno,
                PublishedAt = publishedAt,
                ScheduledStartTime = scheduledStart,
                ActualStartTime = actualStart ?? sysTimeNow,
                ActualEndTime = actualEnd,
                AddedAt = sysTimeNow
            };
            huboCambiosEnVivos = true;
        }
        else if (estabaEnActivos)
        {
            canal.Actives.TryGetValue(videoId, out var videoActivo);
            canal.Actives.Remove(videoId);

            canal.Past[videoId] = new PastVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? videoActivo?.Title ?? "Directo finalizado",
                ThumbnailUrl = liveImageUrl,
                WasPremiere = videoActivo?.IsPremiere ?? false,
                PublishedAt = publishedAt ?? videoActivo?.PublishedAt,
                ScheduledStartTime = scheduledStart ?? videoActivo?.ScheduledStartTime,
                ActualStartTime = actualStart ?? videoActivo?.ActualStartTime,
                ActualEndTime = actualEnd ?? sysTimeNow,
                EndedAt = sysTimeNow
            };

            _logger.LogInformation("FINALIZADO: El video {VideoId} de {ChannelName} pasó a Past.", videoId, canal.ChannelName);
            huboCambiosEnVivos = true;
        }
        else if (estabaEnPast)
        {
            canal.Past.TryGetValue(videoId, out var videoPast);
            canal.Past[videoId] = new PastVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? videoPast?.Title ?? "Directo finalizado",
                ThumbnailUrl = !string.IsNullOrEmpty(liveImageUrl) ? liveImageUrl : videoPast?.ThumbnailUrl,
                WasPremiere = videoPast?.WasPremiere ?? false,
                PublishedAt = publishedAt ?? videoPast?.PublishedAt,
                ScheduledStartTime = scheduledStart ?? videoPast?.ScheduledStartTime,
                ActualStartTime = actualStart ?? videoPast?.ActualStartTime,
                ActualEndTime = actualEnd ?? videoPast?.ActualEndTime,
                EndedAt = videoPast?.EndedAt ?? sysTimeNow
            };
        }

        // 4. GESTIONAR "UPCOMING"
        if (esUpcoming)
        {
            canal.Upcoming[videoId] = new UpcomingVideo
            {
                VideoId = videoId,
                Title = videoInfo?.Snippet?.Title ?? (esEstreno ? "Estreno Programado" : "Directo Programado"),
                ThumbnailUrl = liveImageUrl,
                IsPremiere = esEstreno,
                PublishedAt = publishedAt,
                ScheduledStartTime = scheduledStart,
                ActualStartTime = actualStart,
                ActualEndTime = actualEnd,
                AddedAt = sysTimeNow
            };
            _logger.LogInformation("PROGRAMADO: {ChannelName} tiene upcoming ({VideoId}).", canal.ChannelName, videoId);
        }
        else if (estabaEnUpcoming)
        {
            canal.Upcoming.TryGetValue(videoId, out var videoUpcoming);
            canal.Upcoming.Remove(videoId);

            if (esEnVivo)
            {
                _logger.LogInformation("MUDANZA: El video {VideoId} pasó a EN VIVO.", videoId);
            }
            else
            {
                canal.Past[videoId] = new PastVideo
                {
                    VideoId = videoId,
                    Title = videoInfo?.Snippet?.Title ?? videoUpcoming?.Title ?? "Programación cancelada",
                    ThumbnailUrl = liveImageUrl,
                    WasPremiere = videoUpcoming?.IsPremiere ?? false,
                    PublishedAt = publishedAt ?? videoUpcoming?.PublishedAt,
                    ScheduledStartTime = scheduledStart ?? videoUpcoming?.ScheduledStartTime,
                    ActualStartTime = actualStart,
                    ActualEndTime = actualEnd ?? sysTimeNow,
                    EndedAt = sysTimeNow
                };
                _logger.LogInformation("CANCELADO: El upcoming {VideoId} se canceló y pasó a Past.", videoId);
            }
        }

        // 5. REGISTRAR ACTIVIDAD (Reemplaza al viejo recálculo legacy)
        if (huboCambiosEnVivos)
        {
            canal.LastActivityAt = sysTimeNow;
        }

        // 6. PERSISTIR EN MONGODB (1 sola llamada atómica)
        await _channelsCollection.ReplaceOneAsync(c => c.Id == canal.Id, canal);
        _logger.LogInformation("El canal {ChannelName} fue actualizado con éxito en MongoDB.", canal.ChannelName);
    }
}

// --- MODELOS ---
[BsonIgnoreExtraElements]
public class ZappingChannel
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } // Acá va el "UC..."

    public string ChannelName { get; set; }

    public string ChannelDescription { get; set; }

    public string ChannelCity { get; set; }

    public string ChannelType { get; set; }

    public string ChannelLiveUrl { get; set; }

    public string ChannelImgUrl { get; set; }

    public string ChannelBannerUrl { get; set; }

    public string LastActivityAt { get; set; }

    // Dictionaries (Mongo los mapea perfecto como subdocumentos dinámicos)
    public Dictionary<string, UpcomingVideo> Upcoming { get; set; }

    public Dictionary<string, ActiveVideo> Actives { get; set; }

    public Dictionary<string, PastVideo> Past { get; set; }
}

[BsonIgnoreExtraElements]
public class UpcomingVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ThumbnailUrl { get; set; }
    public bool IsPremiere { get; set; }
    public string PublishedAt { get; set; }
    public string ScheduledStartTime { get; set; }
    public bool ToBeCut { get; set; }
    public string ActualStartTime { get; set; }
    public string ActualEndTime { get; set; }
    public string AddedAt { get; set; }
}

[BsonIgnoreExtraElements]
public class ActiveVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ThumbnailUrl { get; set; }
    public bool IsPremiere { get; set; }
    public string PublishedAt { get; set; }
    public string ScheduledStartTime { get; set; }
    public bool ToBeCut { get; set; }
    public string ActualStartTime { get; set; }
    public string ActualEndTime { get; set; }
    public string AddedAt { get; set; }
}

[BsonIgnoreExtraElements]
public class PastVideo
{
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ThumbnailUrl { get; set; }
    public bool WasPremiere { get; set; }
    public string PublishedAt { get; set; }
    public string ScheduledStartTime { get; set; }
    public bool ToBeCut { get; set; }
    public string ActualStartTime { get; set; }
    public string ActualEndTime { get; set; }
    public string EndedAt { get; set; }
}
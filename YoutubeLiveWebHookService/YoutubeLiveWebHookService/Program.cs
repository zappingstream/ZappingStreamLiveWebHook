using Firebase.Database;
using Firebase.Database.Query;
using Google.Apis.Services;
using Google.Apis.YouTube.v3;
using Microsoft.AspNetCore.Mvc;
using System.Text.RegularExpressions;
using System.Xml.Linq;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// 1. CORRECCIÓN: Usar builder.Configuration
string firebaseUrl = builder.Configuration["Firebase:Url"] ?? "https://zappingstreaming-default-rtdb.firebaseio.com/";
string ytApiKey = builder.Configuration["YouTube:ApiKey"] ?? "";
string firebaseSecret = builder.Configuration["Firebase:Secret"] ?? "";

var options = new FirebaseOptions
{
    AuthTokenAsyncFactory = () => Task.FromResult(firebaseSecret)
};

// Como estamos en top-level statements, estas variables son accesibles por las funciones de abajo
var _firebaseClient = new FirebaseClient(firebaseUrl, options);

var _youtubeService = new YouTubeService(new BaseClientService.Initializer()
{
    ApiKey = ytApiKey,
    ApplicationName = "ZappingStreamingWorker"
});

// Nuestro endpoint que acepta GET y POST
app.MapMethods("/webhook", new[] { "GET", "POST" }, async (HttpContext context) =>
{
    // 1. EL GET: GOOGLE VERIFICANDO QUE EL ENDPOINT EXISTE
    if (context.Request.Method == HttpMethods.Get)
    {
        if (context.Request.Query.TryGetValue("hub.challenge", out var challenge))
        {
            app.Logger.LogInformation("Suscripción verificada por Google.");
            return Results.Content(challenge, "text/plain");
        }
        return Results.BadRequest("Falta el hub.challenge");
    }

    // 2. EL POST: GOOGLE AVISANDO QUE HAY ACTIVIDAD EN UN CANAL
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
                string channelId = channelIdElement?.Value;

                app.Logger.LogInformation("¡Aviso recibido! Actividad en el video ID: {VideoId} del canal ID: {ChannelId}", videoId, channelId);

                await ProcesarVideoAsync(videoId, channelId);
            }
        }
        catch (Exception ex)
        {
            app.Logger.LogWarning("Ignorando XML inválido o distinto: {Message}", ex.Message);
        }

        // 3. RESPUESTA RÁPIDA: Siempre devolver 200 OK
        return Results.Ok();
    }

    return Results.StatusCode(405); // Método no permitido
});

app.Run();

// --- FUNCIONES AUXILIARES ---

async Task ProcesarVideoAsync(string videoId, string channelId)
{
    try
    {
        Console.WriteLine("Procesando webhook para VideoId: {VideoId}", videoId);

        // 1. Consultar a YouTube sobre el estado de ESTE video
        var videoRequest = _youtubeService.Videos.List("snippet,contentDetails");
        videoRequest.Id = videoId;
        var videoResponse = await videoRequest.ExecuteAsync();
        var video = videoResponse.Items?.FirstOrDefault();

        if (video == null) return;

        bool estaEnVivo = video.Snippet?.LiveBroadcastContent == "live";
        string liveImageUrl = estaEnVivo ?
            (video.Snippet.Thumbnails?.High?.Url ?? video.Snippet.Thumbnails?.Medium?.Url ?? "") : "";

        string channelName = video.Snippet.ChannelTitle;
        string firebaseKey = SanitizarKeyFirebase(channelName);

        // 2. LEER EL ESTADO ACTUAL DEL CANAL EN FIREBASE
        var canalEnFirebase = await _firebaseClient
            .Child("Channels")
            .Child(firebaseKey)
            .OnceSingleAsync<FirebaseChannel>();

        // Si el canal no existía en la DB, asumimos valores por defecto
        bool estabaEnVivo = canalEnFirebase?.ChannelLive ?? false;
        string videoVivoActualId = canalEnFirebase?.LiveVideoId ?? "";

        // 3. LÓGICA DE ACTUALIZACIÓN INTELIGENTE
        object actualizacionParcial;

        if (estaEnVivo)
        {
            // A. El aviso es de un stream activo. Prendemos todo y guardamos el ID.
            actualizacionParcial = new
            {
                ChannelLive = true,
                ChannelImgLiveUrl = liveImageUrl,
                LiveVideoId = videoId, // Guardamos el ID del stream
                LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
            };

            Console.WriteLine("Canal {ChannelName} empezó a transmitir en vivo vía Webhook.", channelName);

        }
        else
        {
            // B. El aviso NO es de un vivo (es un video normal, un Short, o un stream que terminó)
            if (estabaEnVivo && videoVivoActualId != videoId && !string.IsNullOrEmpty(videoVivoActualId))
            {
                // ¡EL CASO RARO SALVADO! 
                // El canal está en vivo (con otro ID), pero subieron un Short.
                // Solo actualizamos la actividad, NO apagamos el stream.
                Console.WriteLine("Canal en vivo con otro video ({VideoVivoActualId}). El aviso es de otro video ({VideoId}). Solo actualizamos fecha.", videoVivoActualId, videoId);

                actualizacionParcial = new
                {
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
                };
                Console.WriteLine("Canal {ChannelName} actualizado vía Webhook.", channelName);

            }
            else
            {
                // C. O bien no estaba en vivo desde antes, o el aviso es del stream actual que acaba de terminar (videoVivoActualId == videoId).
                // Apagamos con confianza y actualizamos actividad.
                actualizacionParcial = new
                {
                    ChannelLive = false,
                    ChannelImgLiveUrl = "",
                    LiveVideoId = "", // Limpiamos el ID
                    LastActivityAt = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
                };
                Console.WriteLine("Canal {ChannelName} pasó a off el streaming vía Webhook.", channelName);

            }
        }

        // 4. Mandamos el parche a Firebase
        await _firebaseClient
            .Child("Channels")
            .Child(firebaseKey)
            .PatchAsync(actualizacionParcial);

    }
    catch (Exception ex)
    {
        Console.WriteLine("Error al procesar el webhook del video {VideoId}", videoId);
    }
}
string SanitizarKeyFirebase(string key)
{
    if (string.IsNullOrWhiteSpace(key)) return "UnknownChannel";
    return Regex.Replace(key, @"[.#$\[\]]", "").Trim();
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
    public string LiveVideoId { get; set; } // ¡NUEVO!
}
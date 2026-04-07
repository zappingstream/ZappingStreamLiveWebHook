using Microsoft.AspNetCore.Mvc;
using System.Xml.Linq;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Nuestro endpoint "hubhubwuhuhu" que acepta GET y POST
app.MapMethods("/webhook", new[] { "GET", "POST" }, async (HttpContext context) =>
{
    // 1. EL GET: GOOGLE VERIFICANDO QUE EL ENDPOINT EXISTE
    if (context.Request.Method == HttpMethods.Get)
    {
        // El hub manda un parámetro "hub.challenge"
        if (context.Request.Query.TryGetValue("hub.challenge", out var challenge))
        {
            Console.WriteLine("Suscripción verificada por Google.");
            // Hay que devolver el texto plano exacto que nos mandaron
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
            // Parseamos el XML usando XDocument
            var xdoc = XDocument.Parse(xmlBody);

            // El namespace específico que usa YouTube en su feed Atom
            XNamespace yt = "http://www.youtube.com/xml/schemas/2015";

            // Buscamos la etiqueta <yt:videoId>
            var videoIdElement = xdoc.Descendants(yt + "videoId").FirstOrDefault();

            if (videoIdElement != null)
            {
                string videoId = videoIdElement.Value;
                Console.WriteLine($"¡Aviso recibido! Actividad en el video ID: {videoId}");

                // Llamamos a un método que haga la consulta a la API de YouTube
                // y luego sincronice con Firebase.
                await ProcesarVideoAsync(videoId);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ignorando XML inválido o distinto: {ex.Message}");
        }

        // 3. RESPUESTA RÁPIDA: Siempre devolver 200 OK para que el Hub no te penalice
        return Results.Ok();
    }

    return Results.StatusCode(405); // Método no permitido
});

app.Run();

// --- FUNCIONES AUXILIARES ---

async Task ProcesarVideoAsync(string videoId)
{
    Console.WriteLine($"Consultando API de YouTube (videos.list) para el ID: {videoId}...");

    // 1. Chequear Firebase: ¿Ya tengo este video guardado como EN VIVO?
    // Si la respuesta es SÍ -> Entonces este aviso seguro es porque el vivo TERMINÓ o le cambiaron el título.

    // 2. Gastar 1 unidad de cuota:
    // Haces un GET a: https://www.googleapis.com/youtube/v3/videos?part=snippet&id={videoId}&key=TU_API_KEY

    // 3. Evaluar el snippet.liveBroadcastContent:
    // Si es "live" -> Guardar en Firebase como Vivo activo.
    // Si es "none" -> Ignorar (es un Short) o marcar en Firebase como finalizado (si ya estaba vivo).
}
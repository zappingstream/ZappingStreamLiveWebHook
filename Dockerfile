# --- ETAPA 1: COMPILACIÓN (Usa el SDK pesado) ---
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# 1. Copiamos el .csproj manteniendo la estructura, pero con barras de Linux (/)
COPY ["YoutubeLiveWebHookService/YoutubeLiveWebHookService/YoutubeLiveWebHookService.csproj", "YoutubeLiveWebHookService/YoutubeLiveWebHookService/"]
RUN dotnet restore "YoutubeLiveWebHookService/YoutubeLiveWebHookService/YoutubeLiveWebHookService.csproj"

# 2. Copiamos el resto del código y compilamos la versión Release
COPY . .
RUN dotnet publish "YoutubeLiveWebHookService/YoutubeLiveWebHookService/YoutubeLiveWebHookService.csproj" -c Release -o /app/publish /p:UseAppHost=false

# --- ETAPA 2: PRODUCCIÓN (Usa el Runtime liviano) ---
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app

# Exponemos el puerto 8080 (el estándar nuevo en .NET 8)
EXPOSE 8080
ENV ASPNETCORE_HTTP_PORTS=8080

# Traemos el código ya compilado de la etapa anterior
COPY --from=build /app/publish .

# Le decimos cómo arrancar (la DLL queda en la raíz gracias al publish)
ENTRYPOINT ["dotnet", "YoutubeLiveWebHookService.dll"]
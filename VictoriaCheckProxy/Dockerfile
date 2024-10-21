#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
USER app
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
# Install clang/zlib1g-dev dependencies for publishing to native
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    clang zlib1g-dev
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["VictoriaCheckProxy/VictoriaCheckProxy.csproj", "VictoriaCheckProxy/"]
RUN dotnet restore "./VictoriaCheckProxy/VictoriaCheckProxy.csproj"
COPY . .
WORKDIR "/src/VictoriaCheckProxy"
RUN dotnet build "./VictoriaCheckProxy.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./VictoriaCheckProxy.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=true && ls -la /app/publish/

# FROM mcr.microsoft.com/dotnet/runtime-deps:8.0 AS final
FROM alpine:3.20
WORKDIR /app
COPY --from=publish /app/publish/VictoriaCheckProxy .
EXPOSE 8801
ENTRYPOINT ["./VictoriaCheckProxy"]
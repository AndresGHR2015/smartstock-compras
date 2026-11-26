FROM mcr.microsoft.com/dotnet/sdk:10.0.100 AS build
WORKDIR /src
COPY src/SmartStock.Compras/SmartStock.Compras.csproj src/SmartStock.Compras/
RUN dotnet restore src/SmartStock.Compras/SmartStock.Compras.csproj
COPY src/ src/
COPY vendor/smartstock-infra/contracts/ vendor/smartstock-infra/contracts/
RUN dotnet publish src/SmartStock.Compras/SmartStock.Compras.csproj -c Release --no-restore -o /app

FROM mcr.microsoft.com/dotnet/runtime:10.0.0
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "SmartStock.Compras.dll"]

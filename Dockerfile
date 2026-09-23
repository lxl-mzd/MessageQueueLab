# ════════════════════════════════════════════════════════
#  用法：把自造消息队列打成 Docker 镜像的说明书
#  用法=图纸：Lin｜两个车间 = 工地（编辑器）→ 成品车间（集装箱）
# ════════════════════════════════════════════════════════

# ── 第 1 段：工地车间（只负责编译，车间用完就拆）──
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish -c Release -o /app/publish

# ── 第 2 段：成品车间（瘦身的最终运行环境）──
# 注意：Web 服务要用 aspnet 变体镜像（自带 ASP.NET Core 运行时）
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "MessageQueueLab.dll"]

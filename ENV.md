# 自造消息队列 · 开发与部署环境清单

> 生成于 2026-09-29，对应 mq-lab v5（commit 94c58dd）
> 以下版本全部经过实机验证（开发机 Windows + 容器内 Linux 双环境）

---

## 一、开发环境（写代码 / 编译 / 实测通过的配置）

### 1.1 语言与运行时

| 组件 | 版本 | 说明 |
|---|---|---|
| **.NET SDK** | **10.0.400** | 主力开发 SDK（`dotnet build/test/run`） |
| .NET SDK（兼容） | 8.0.424 | 机器上还装的旧 LTS（本项目不依赖） |
| **TargetFramework** | **net10.0** | csproj 的编译目标 |
| **C# 语言版本** | 最新（随 SDK） | csproj 未锁定，跟随 SDK 默认 |
| **ASP.NET Core** | 10.0.11 runtime | SDK 自带，Kestrel HTTP 服务 |
| .NETCore.App 10.0.11 | 运行时 | |
| ImplicitUsings / Nullable | enable / enable | csproj 顶级配置 |

### 1.2 Docker 全家（开开发机实测版本）

| 组件 | 版本 |
|---|---|
| **Docker Desktop (Windows)** | **4.90.0 (238679)** |
| Docker Engine | **29.7.2**（API 1.55，最低 1.40） |
| Go version (docker 由 Go 写) | go1.26.5 |
| **Docker Compose** | **v5.5.1** |
| containerd | v2.3.3 |
| runc | 1.4.3 |
| docker-init | 0.19.0 |
| OS/Arch（server 端） | linux/amd64（daemon 跑在 WSL2/虚拟机里） |

### 1.3 Kubernetes（可选，扩缩容用）

| 组件 | 版本 |
|---|---|
| kubectl 客户端 | v1.36.1 |
| Docker Desktop 内嵌 K8s 服务端 | v1.36.1（开关在 Docker Desktop GUI 里启用） |
| kind 集群名 | desktop-control-plane |

> ⚠️ K8s 是 Docker Desktop 的可选组件，不开也能跑单机/compose；只做"自动扩缩容"时才开。

### 1.4 操作系统

| 环境 | 版本 |
|---|---|
| 开发机 | Windows（个人电脑，含 WSL2 后端） |
| 容器内 | Debian/Ubuntu（`mcr.microsoft.com/dotnet/aspnet:10.0` 官方镜像） |

### 1.5 测试框架

| 包 | 版本 |
|---|---|
| xunit | 2.9.0 |
| xunit.runner.visualstudio | 2.8.2 |
| Microsoft.NET.Test.Sdk | 17.11.1 |

---

## 二、部署环境（别人电脑上要装什么 / 版本要求）

### 2.1 最低要求清单

| 项目 | 最低版本 | 推荐版本 | 为什么 |
|---|---|---|---|
| **Docker Desktop**（Windows/Mac）或 Docker Engine（Linux） | 4.30+ / Engine 20.10+ | 4.90+ / 29.x（我们实测的） | compose v2 的 YAML 语法 / volumeClaimTemplates 兼容性 |
| **Docker Compose** 插件 | v2.20+ | v5.5.1（实机测试版） | YAML anchor 语法（`&mq-peers` / `*mq-peers`） |
| 磁盘空间 | 500MB | 1GB+ | 镜像 341MB + WAL 增长空间 |
| 内存 | 512MB | 1GB+ | 单容器 .NET 最低运行需求 |
| 可用端口 | 1 个 | — | 默认 5000，冲突可换 |

### 2.2 不需要装的东西（零依赖的卖点）

- ❌ .NET SDK / Runtime（容器里自带）
- ❌ ZooKeeper / etcd（Raft 自己管理）
- ❌ Redis / 数据库（WAL 就是存储）
- ❌ Git / IDE（部署时都不用）

### 2.3 三种部署形态的环境清单（递增）

```bash
# 形态 1：单机（最简）
必须：docker 20.10+
一条命令：docker run -d --name mq-server -p 5000:8080 -v mq-data:/app/data \
    ghcr.io/lxl-mzd/messagequeue-lab:latest

# 形态 2：compose 三节点（小团队生产）
必须：docker compose v2.20+
两条命令：git clone + docker compose up -d --build

# 形态 3：K8s StatefulSet（自动扩缩容）
必须：任何 K8s 集群（Docker Desktop 内嵌 / k3s / kind / 云托管）
三条命令：kubectl apply -f k8s/xxx.yaml × 3
```

### 2.4 CG 镜像 & 加速

- 海外部署直接 `docker pull ghcr.io/lxl-mzd/messagequeue-lab:latest`
- 国内若 GHCR 慢，可配置 Docker 镜像加速（DaoCloud 非白名单可能出现 `image mq-lab:18` 报错 — 处理方式：手动 `docker build -t mq-lab:18 .` 即可）

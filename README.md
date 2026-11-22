<p align="center">
  <img src="imgs/logo.png" alt="Openlist-Ani Logo" width="420" />
</p>

<h1 align="center">Openlist-Ani</h1>

<p align="center">
  <b>🎬 RSS 订阅 → 自动下载 → AI 重命名，一站式番剧自动化管理</b>
</p>

<p align="center">
  Openlist-Ani，简称O-Ani，从动漫番剧相关的 RSS 订阅源中自动获取番剧更新，通过 Openlist 离线下载至对应网盘，<br/>
  并结合 LLM 分析资源名，将资源重命名为 Emby / Jellyfin 可解析的格式。
</p>

---

## 🔧 本 Fork 改动说明（基于 TwooSix/Openlist-Ani）

本仓库在原版基础上新增以下改动（不改变原有 API，完全向后兼容）：

### 1. 支持空 RSS 启动（仅通过 API 手动提交磁力任务）

- 允许 `[rss]` 中 `urls = []`（或省略 `[rss]`）直接启动，不再因缺少 RSS 链接报错退出。
- 仍保留对无效 RSS 配置的校验（如空白 URL、`interval_time <= 0`、无效正则/优先级字段等）。
- `rss.urls` 为空时跳过订阅轮询抓取，手动下载 API（`POST /api/downloads` 等）、PikPak 离线下载、番剧识别与重命名功能保持正常；后续也可通过 `POST /api/rss` 动态添加订阅恢复抓取。

### 2. 下载整理后自动复制到 OpenList 其他文件夹并删除源文件

- 在 `[downloader.openlist]` 新增以下可选配置（对所有 OpenList 离线下载工具生效，包括 PikPak）：
  - `copy_target_paths`：目标文件夹路径列表（默认 `[]`，为空时不启用复制；支持配置一个或多个路径，禁止与 `downloader.download_path` 相同）。
  - `delete_source_after_copy`：所有目标复制校验完成后是否删除原下载目录中的源文件（默认 `true`，节省 PikPak 等中转网盘空间；如需保留源文件可设为 `false`）。
- 工作流程：番剧识别、重命名并整理到 `<download_path>/<番剧名>/Season <季>/` 后，按相同的 `<番剧名>/Season <季>/` 子目录结构将视频和关联字幕复制到所有目标文件夹。
- 安全与容错：兼容 OpenList 跨存储异步复制（轮询 `/api/task/copy/*` 等待完成）和同存储同步复制，支持中断后断点恢复；**只有全部目标目录均复制成功并校验文件存在且大小一致后**，才会删除源文件并更新任务 `final_path`，任何目标失败均会重试且绝不提前删除源文件。

### 3. 自动构建 AMD64/ARM64 GHCR 镜像（支持 1Panel V2 部署）

- 新增 [`.github/workflows/docker-ghcr.yml`](.github/workflows/docker-ghcr.yml)：推送 `master`、`v*` 标签或手动触发时，自动原生构建 `linux/amd64` 与 `linux/arm64` 并发布至 GHCR。
- **镜像地址**：
  - `ghcr.io/666aaa70/openlist-ani:latest`（跟踪 `master` 最新版）
  - `ghcr.io/666aaa70/openlist-ani:master` / `ghcr.io/666aaa70/openlist-ani:sha-<commit>`

#### 快速配置示例（空 RSS + PikPak 离线下载 + 自动复制到目标目录）

```toml
config_version = 2

[backend]
host = "0.0.0.0" # 供宿主机或同网络其他容器调用 API
port = 26666

[rss]
urls = []

[downloader]
download_path = "/PikPak/Anime"
rename_format = "{anime_name} S{season:02d}E{episode:02d} {fansub} {quality} {languages}"

[downloader.openlist]
url = "http://openlist:5244"
token = "你的OpenList令牌"
offline_download_tool = "PikPak"
copy_target_paths = ["/115/Anime"] # 支持多个目标路径，如 ["/115/Anime", "/Local/Anime"]
delete_source_after_copy = true    # 复制校验完成后自动删除 PikPak 原文件

[metadata]
pipeline = ["regex", "tmdb"]
```

#### 1Panel V2 Docker Compose 示例

> 容器以 UID/GID `1000:1000` 运行，首次部署前请先创建文件并授权：
> ```bash
> mkdir -p /opt/1panel/apps/openlist-ani/{data,logs,skills}
> touch /opt/1panel/apps/openlist-ani/config.toml
> chown -R 1000:1000 /opt/1panel/apps/openlist-ani
> ```

```yaml
services:
  openlist-ani:
    image: ghcr.io/666aaa70/openlist-ani:latest
    container_name: openlist-ani
    restart: unless-stopped
    ports:
      - "26666:26666"
    environment:
      - TZ=Asia/Shanghai
      - CONFIG_PATH=/config.toml
      - ENABLE_ASSISTANT=false
    volumes:
      - /opt/1panel/apps/openlist-ani/config.toml:/config.toml
      - /opt/1panel/apps/openlist-ani/data:/data
      - /opt/1panel/apps/openlist-ani/logs:/logs
      - /opt/1panel/apps/openlist-ani/skills:/skills
    networks:
      - 1panel-network

networks:
  1panel-network:
    external: true
```

更新镜像：

```bash
docker compose pull && docker compose up -d
```

---

## ✨ 特点

- 📡 **自动追番** — 自动获取 RSS 番剧更新并下载
- 📦 **多网盘支持** — 基于OpenList实现，支持 PikPak、115 等离线下载
- 🗂️ **合集整理** — 一次下载多集文件，逐集解析元数据、匹配字幕并归类到媒体库
- 🤖 **AI 重命名** — AI 分析资源名 + TMDB 搜索，精准获取番剧名、季度、集数
- 💬 **智能助理** — 挂载至 Telegram / 微信ClawBot / 飞书机器人，通过自然语言让 AI 帮你搜索并下载资源
- 🔔 **更新通知** — 通过 PushPlus、Telegram、微信ClawBot、飞书等渠道推送更新

## 📋 准备工作

1. 参照 [Openlist 官方文档](https://doc.oplist.org/guide) 部署 Openlist，并搭建好离线下载
2. 准备好 RSS 订阅链接（如 [Mikan Project](https://mikanani.me)）
3. 可选：准备 AI API Key，或安装并登录 Pi / Claude Code / Codex Agent（均未配置时元数据使用本地正则 + TMDB）

非 Docker 安装支持 Python 3.11、3.12 和 3.13，使用 `libtorrent>=2.0.13,<2.1`
（当前锁定版本为 2.0.13）。Windows x64 会在这三个 Python 版本上执行兼容性测试；
如果本机原生模块导入失败，请先升级到受支持的 Python 和最新项目版本，再按
[PIP 安装指南](docs/pip-installation.md)中的 Windows 检查步骤验证。

源码或 PIP 安装无需 Node.js/npm。首次使用默认 Pi Assistant 时，程序会下载
官方锁定版本的独立二进制，校验 SHA-256 后原子安装到配置文件旁的
`data/assistant/runtime/pi/`。显式配置的 `executable` 和 PATH 中已有的 Pi
仍然优先，不会被覆盖。Windows 如果没有可用的 Bash，程序还会下载官方
PortableGit、校验固定 SHA-256，并将 Git Bash 安装到
`data/assistant/runtime/git-bash/`；不会修改系统安装或全局 PATH。

API source（包括旧 `[llm]` 自动迁移得到的 source）会自动注入 Pi，无需重新
配置模型；完全没有 AI source 时才使用 Pi 自己的登录和模型配置。Docker 镜像
已预装 Pi。如果使用 Claude Code 或 Codex source，则安装并登录对应的原生 CLI。
内置 Skills 不再复制到临时会话目录：Pi 直接使用 `--skill`，Claude Code 使用
`--plugin-dir`，Codex 则在每个隔离会话的 `.agents/skills` 中建立指向
随包 Skill 目录的临时符号链接，不复制内容或修改 Codex 的持久配置。

## 🚀 快速开始

<details open>
<summary><b>方式一：源码安装（推荐）</b></summary>

#### 前置：安装 uv

<table><tr><td>

**Linux / macOS**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

</td><td>

**Windows**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

</td></tr></table>

> 更多安装方式参考 [uv 官方文档](https://github.com/astral-sh/uv)

**1. 安装**

```bash
git clone https://github.com/TwooSix/Openlist-Ani.git
cd Openlist-Ani

# 推荐切换到最新 Release tag；master 为开发分支，不保证稳定
# Release 版本以 tag 为准
git fetch --tags
git checkout tags/v***    # 将 v*** 替换为 Release 页面中的版本号

uv sync --no-dev --frozen
```

**2. 创建配置文件**

在运行目录下新建 `config.toml`，填入以下内容：

```toml
config_version = 2

[rss]
urls = ["RSS订阅链接"]

[downloader]
download_path = "/PikPak/Anime"
rename_format = "{anime_name} S{season:02d}E{episode:02d} {fansub} {quality} {languages}"
# 可选年份："{anime_name} ({year}) S{season:02d}E{episode:02d}"

[downloader.openlist]
url = "http://localhost:5244"
token = ""                          # 令牌，见「设置 → 其他 → 令牌」
offline_download_tool = "qBittorrent"

[metadata]
pipeline = ["regex", "tmdb"]       # 默认配置；推荐配置 AI source 后改为 ["ai", "tmdb"]

# 可选：API source。Metadata 直接调用 API，Assistant 由内置 Pi 承载。
# [ai.sources.primary]
# type = "api"
# provider = "openai-compatible"
# api_key = "sk-xxx"
# base_url = "https://api.deepseek.com/v1"
# model = "deepseek-chat"
```

> 完整配置项请参考 [`config.toml.example`](config.toml.example) 及 [配置说明](https://github.com/TwooSix/Openlist-Ani/wiki/configuration)

**3. 启动**

```bash
uv run openlist-ani
```

**4.（可选）启动 AI 助理**

在配置文件中补充助理配置后运行：

```toml
[assistant]
enabled = true

[assistant.telegram]
enabled = true
bot_token = ""        # 从 @BotFather 获取
allowed_users = [123456789]  # 必填；只有这些用户可以使用 Assistant
```

```bash
uv run openlist-ani-assistant
```

内置 Skills 会随程序包加载并由 Agent 原生发现。`./skills` 可向 Pi/Claude Code
直接提供自定义 Skills；Codex 会将其与内置 Skills 一起投影到隔离会话的
`.agents/skills` 目录，同名 Skill 目录以用户版本为准。

</details>

<details>
<summary><b>方式二：Docker 部署</b></summary>

**1. 准备文件**

在运行目录下创建：
- `config.toml` — 配置文件（内容同上）
- `data/` — 数据目录

**2. 启动容器**

```bash
docker run -d \
  --name openlist-ani \
  --network host \
  -e ENABLE_ASSISTANT=false \
  -v /path/to/config.toml:/config.toml \
  -v /path/to/data:/data \
  twosix26/openlist-ani:latest
```

> 将 `/path/to/` 替换为你的实际路径。  
> 如需启用 AI 助理，填写好配置后将 `ENABLE_ASSISTANT` 设为 `true`。  
> 详细说明见 [Docker 部署指南](https://github.com/TwooSix/Openlist-Ani/wiki/docker-deployment)。

</details>

<details>
<summary><b>方式三：PIP 安装</b></summary>

```bash
pip install openlist-ani
openlist-ani

# 可选：启动 AI 助理
openlist-ani-assistant
```

</details>

## 📖 文档

- [快速开始](https://github.com/TwooSix/Openlist-Ani/wiki/quick-start)
- [PIP 安装指南](https://github.com/TwooSix/Openlist-Ani/wiki/pip-installation)
- [Docker 部署指南](https://github.com/TwooSix/Openlist-Ani/wiki/docker-deployment)
- [源码编译指南](https://github.com/TwooSix/Openlist-Ani/wiki/build-from-source)
- [配置说明](https://github.com/TwooSix/Openlist-Ani/wiki/configuration)

## 🖼️ 效果展示

| 重命名结果 | 智能助理 |
| :---: | :---: |
| <img src="https://github.com/TwooSix/Openlist-Ani/blob/master/imgs/show_pic1.png" width="400"/> | <img src="https://github.com/TwooSix/Openlist-Ani/blob/master/imgs/show_pic2.jpg" width="150"/> |

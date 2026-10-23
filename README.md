# Task Bridge

用 Go 实现的自托管任务桥。Claude Code（A）连接远程 WebSocket MCP，B 以 stdio MCP 客户端接入其他宿主，并在本机指定工作区执行 Claude 发出的 Bash 命令。服务端保存任务、命令、输出及恢复信息。

源码仓库：[Zxilly/task-bridge](https://github.com/Zxilly/task-bridge)。

当前交付为开发版本。按用户确认，真实 Claude Code channel、目标生产部署/TLS 和实际拟用宿主的验收留待后续。Go 实现与 Windows/Linux 自动化测试的范围见 [compatibility.md](docs/compatibility.md)；协议客户端测试不代表真实 Claude channel 已通过。

## 组成

- `task-bridge-server`：WebSocket MCP、B 控制连接、SQLite 持久化、调度及本机管理 API。
- `task-bridge-client`：stdio MCP、B 控制连接、本地 journal 与 Bash 执行器。
- `deploy/`：单 broker 的 Compose、Caddy 和非 root 容器构建文件。

应用代码统一使用 Go 1.26。MCP 使用 [官方 Go SDK](https://github.com/modelcontextprotocol/go-sdk)，WebSocket 使用 [coder/websocket](https://github.com/coder/websocket)，SQLite 使用 [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)，迁移使用 [Goose](https://github.com/pressly/goose)，进程锁使用 [gofrs/flock](https://github.com/gofrs/flock)，CLI 使用 [Cobra](https://github.com/spf13/cobra)。依赖版本记录在 `go.mod` 和 `go.sum`。

## 预编译包

当前尚未发布 GitHub Release。平台包同时包含源码、文档和根目录下的 `task-bridge-server` / `task-bridge-client`（Windows 为 `.exe`）。解压后可以直接运行 `./task-bridge-server --help` 或 `./task-bridge-client --help`，配置步骤与下文相同；将命令示例中的 `bin/task-bridge-server`、`bin/task-bridge-client` 换为包内路径。

Windows/Linux 的 amd64 包已执行版本 smoke；arm64 包仅完成交叉构建。具体范围见 [验证记录](docs/compatibility.md)。本轮实现和保留验收项见 [实现状态](docs/implementation-status.md)。

## 从本地源码构建

在仓库根目录运行：

```sh
go mod download
go build -trimpath -o bin/task-bridge-server ./cmd/task-bridge-server
go build -trimpath -o bin/task-bridge-client ./cmd/task-bridge-client
go test ./...
go vet ./...
```

Windows 构建时为输出文件加上 `.exe`。B 需要安装 Bash；Windows 使用 Git for Windows 提供的 Bash。服务端自身不执行工作区命令。

## 配置服务端

复制 `examples/server-config.json` 为 `server.json`，设置独立的管理 token，再启动服务端。以下命令在项目根目录运行：

```sh
cp examples/server-config.json server.json
export BRIDGE_ADMIN_TOKEN="$(openssl rand -hex 32)"
bin/task-bridge-server serve --config server.json
```

另一个终端需要设置同一个 `BRIDGE_ADMIN_TOKEN`。先注册工作区和两类设备，再分别保存签发的 token：

```sh
bin/task-bridge-server admin --config server.json workspaces create --id demo --executor laptop
bin/task-bridge-server admin --config server.json devices create --kind executor --executor laptop --workspace demo
bin/task-bridge-server admin --config server.json devices create --kind claude --agent claude-demo --workspace demo
```

设备 token 与管理 token 分开。A token 只用于 Claude；B token 只用于对应 executor。不要把 token 写入命令行参数或提交到版本库。

Windows PowerShell 的服务端启动写法：

```powershell
Copy-Item examples/server-config.json server.json
$env:BRIDGE_ADMIN_TOKEN = [Convert]::ToHexString([System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
.\bin\task-bridge-server.exe serve --config server.json
```

管理命令同样使用 `.\bin\task-bridge-server.exe`；执行管理命令的终端需加载相同管理 token。

开发配置默认监听 `127.0.0.1:8080`，管理 API 监听 `127.0.0.1:8081`。部署到服务器时使用 [运维步骤](docs/operations.md) 和 `examples/server-container.json`。

## 配置 B 与 A

B 将逻辑 `workspace_id` 映射到本地目录，并在 Hello 中上报本次配置的工作区子集。未上报工作区的任务保持 queued，该工作区的新 Bash 请求被拒绝。使用 `examples/client-config.json`（Linux）或 `examples/client-config.windows.json`（Windows）建立自己的 `client.json`，在 MCP 宿主中加载 `task-bridge-client mcp --config <绝对路径>`。stdio 的 stdout 由 MCP 独占，诊断日志写入 stderr。

```sh
bin/task-bridge-client doctor --config client.json
bin/task-bridge-client mcp --config client.json
```

宿主配置见 `examples/host-mcp.json`。同一 executor/journal 同时只能由一个 B 进程使用；第二个宿主需要独立配置，或先关闭已有进程。

桌面 MCP 宿主需要实际继承 `BRIDGE_EXECUTOR_TOKEN`。若不方便为宿主设置环境变量，在 `client.json` 删除 `token_env`，改用 `"token_file": "./executor.token"`，将签发的 B token 单独保存到该文件并限制文件权限。不要同时设置这两个字段。

A 配置见 `examples/claude-mcp.json`，将 URL 改为实际 WSS 地址，并在 Claude 环境设置 `BRIDGE_AGENT_TOKEN`。开发时可改为 `ws://127.0.0.1:8080/mcp/agent`。channel 预览期准入、启动参数和验证步骤见 [兼容性记录](docs/compatibility.md)。

B 工具包括 `tasks_submit`、`tasks_run`、`tasks_get`、`tasks_wait`、`tasks_cancel`、`tasks_list` 和 `tasks_events`。A 收到任务通知后调用 `task_accept`，使用远程 `bash` 工具，再调用 `task_complete` 保存结果。完整工具字段及恢复语义见 [协议文档](docs/protocol.md)。

## 行为边界

- 一台服务端进程配合本地 SQLite；一个 A 会话固定工作区，工作区串行处理任务。
- 等待请求结束或断线不等于任务结束。使用返回的 `task_id`、`command_id` 查询已保存的状态。
- 相同逻辑命令重试复用 `operation_key`。结果不明时先查询；更换 key 无法避免重复副作用。
- 无法确认是否启动或是否完成的命令进入 `unknown`，需要核对。任意 Bash 副作用不具备 exactly-once 保证。
- 恢复旧服务端快照后，先使用 `task-bridge-client reconcile` 核对保留的 journal，再由管理员处理未知结果和工作区隔离。
- B 的命令使用配置账号的操作系统权限；工作区路径约束不构成隔离不互信代码的沙箱。
- 首版不提供 PTY、交互 stdin、持久 shell、后台 daemon、多实例服务端或 Streamable HTTP MCP。

## 文档与交付

- [架构](docs/architecture.md)：组件、身份、状态和故障处理。
- [协议](docs/protocol.md)：工具、B 控制消息、幂等与输出。
- [运维](docs/operations.md)：本地运行、Compose、管理、备份与恢复。
- [兼容性](docs/compatibility.md)：验证状态、真实 channel smoke 和平台限制。

CI 定义 Windows/Linux 的测试、vet 和构建，另设 Linux race 与容器构建，运行记录见 [GitHub Actions](https://github.com/Zxilly/task-bridge/actions)。`.goreleaser.yaml` 提供本地归档配置，默认关闭远程 Release 发布。

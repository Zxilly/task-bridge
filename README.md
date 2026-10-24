# Task Bridge

用 Go 实现的自托管任务桥。A 端控制器连接远程 WebSocket MCP，为每个新任务启动一个独立 Claude Code 会话，并支持显式恢复同一任务的原会话；B 以 stdio MCP 客户端接入其他宿主，并在本机指定工作区执行 Claude 发出的 Bash 命令。服务端保存任务、命令、输出及恢复信息。

源码仓库：[Zxilly/task-bridge](https://github.com/Zxilly/task-bridge)。

当前交付为开发版本。真实 Claude 账号的逐任务运行、可选手工 channel、目标生产部署/TLS 和实际拟用宿主的验收范围见 [compatibility.md](docs/compatibility.md)。模拟启动器和协议客户端测试不代表真实 Claude 模型运行已通过。

## 组成

- `task-bridge-server`：WebSocket MCP、B 控制连接、SQLite 持久化、调度及本机管理 API。
- `task-bridge-agent`：A 端控制器、每任务独立 Claude 进程，以及只服务当前任务的本机 HTTP MCP 转发。
- `task-bridge-client`：stdio MCP、B 控制连接、本地 journal 与 Bash 执行器。
- `deploy/`：单 broker 的 Compose、Caddy 和非 root 容器构建文件。

应用代码统一使用 Go 1.26。MCP 使用 [官方 Go SDK](https://github.com/modelcontextprotocol/go-sdk)，WebSocket 使用 [coder/websocket](https://github.com/coder/websocket)，SQLite 使用 [modernc.org/sqlite](https://pkg.go.dev/modernc.org/sqlite)，迁移使用 [Goose](https://github.com/pressly/goose)，进程锁使用 [gofrs/flock](https://github.com/gofrs/flock)，CLI 使用 [Cobra](https://github.com/spf13/cobra)。依赖版本记录在 `go.mod` 和 `go.sum`。

## 预编译包

当前尚未发布 GitHub Release。GoReleaser 平台归档配置包含文档、示例和 `task-bridge-server`、`task-bridge-agent`、`task-bridge-client`（Windows 为 `.exe`）。解压后使用包内相应程序的 `--help`，将下文命令中的 `bin/` 路径换为实际位置。

已有服务端与 B 客户端的 Windows/Linux amd64 包执行过版本 smoke，arm64 包仅完成交叉构建。新 A 控制器的检查与真实运行边界另见 [验证记录](docs/compatibility.md)。

## 从本地源码构建

在仓库根目录运行：

```sh
go mod download
go build -trimpath -o bin/task-bridge-server ./cmd/task-bridge-server
go build -trimpath -o bin/task-bridge-agent ./cmd/task-bridge-agent
go build -trimpath -o bin/task-bridge-client ./cmd/task-bridge-client
go test ./...
go vet ./...
```

Windows 构建时为输出文件加上 `.exe`。A 需要可用的 Claude Code 安装和登录；B 需要安装 Bash，Windows 使用 Git for Windows 提供的 Bash。服务端自身不执行工作区命令。

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

设备 token 与管理 token 分开。A token 用于对应 A 控制器，B token 只用于对应 executor。它们都不是 Claude 账号的登录凭据。不要把 token 写入命令行参数或提交到版本库。

Windows PowerShell 的服务端启动写法：

```powershell
Copy-Item examples/server-config.json server.json
$env:BRIDGE_ADMIN_TOKEN = [Convert]::ToHexString([System.Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
.\bin\task-bridge-server.exe serve --config server.json
```

管理命令同样使用 `.\bin\task-bridge-server.exe`；执行管理命令的终端需加载相同管理 token。

开发配置默认监听 `127.0.0.1:8080`，管理 API 监听 `127.0.0.1:8081`。部署到服务器时使用 [运维步骤](docs/operations.md) 和 `examples/server-container.json`。

## 让 agent 自行配置

运行中的服务提供两份英文 Markdown 指南，可将相应 URL 交给该端 agent：

- `/setup/claude.md`：[A 端配置指南](internal/server/setup/claude.md)，复用 Claude 登录并配置逐任务会话控制器。
- `/setup/client.md`：[本地执行端（B）配置指南](internal/server/setup/client.md)，配置本地工作区、执行器及 stdio MCP 宿主。

例如，部署在 `https://bridge.example.com` 时，将 `https://bridge.example.com/setup/claude.md` 或 `https://bridge.example.com/setup/client.md` 提供给对应 agent。指南随服务二进制嵌入，Caddy 模板已开放这两个入口。

还需分别提供管理员已签发的 A 或 B 设备 token，以及工作区等实例信息。指南不包含凭据；agent 只配置本端角色，并在修改已有配置前保留原内容。直接读取源码中的指南时，需另行指定实际服务地址。

## 配置 B 与 A

B 将逻辑 `workspace_id` 映射到本地目录，并在 Hello 中上报本次配置的工作区子集。未上报工作区的任务保持 queued，该工作区的新 Bash 请求被拒绝。使用 `examples/client-config.json`（Linux）或 `examples/client-config.windows.json`（Windows）建立自己的 `client.json`，在 MCP 宿主中加载 `task-bridge-client mcp --config <绝对路径>`。stdio 的 stdout 由 MCP 独占，诊断日志写入 stderr。

```sh
bin/task-bridge-client doctor --config client.json
bin/task-bridge-client mcp --config client.json
```

宿主配置见 `examples/host-mcp.json`。同一 executor/journal 同时只能由一个 B 进程使用；第二个宿主需要独立配置，或先关闭已有进程。

桌面 MCP 宿主需要实际继承 `BRIDGE_EXECUTOR_TOKEN`。若不方便为宿主设置环境变量，在 `client.json` 删除 `token_env`，改用 `"token_file": "./executor.token"`，将签发的 B token 单独保存到该文件并限制文件权限。不要同时设置这两个字段。

A 先在运行控制器的系统账号下完成 Claude 登录；已有可用登录无需重新认证。复制 `examples/agent-config.json` 为 `agent.json`，将 `server_url` 改为实际 `/mcp/agent` WSS 地址，并设置独立的 `BRIDGE_AGENT_TOKEN`。也可用 `token_file` 替换 `token_env`，规则与 B 相同。

```sh
claude auth login
bin/task-bridge-agent doctor --config agent.json
bin/task-bridge-agent run --config agent.json
```

`doctor` 只检查本地配置、Claude 路径和设备凭据是否存在，不启动 Claude、不验证账号登录，也不连接 broker。`run` 保持在线，由控制器接收 channel 通知。新任务使用新 UUID、独立工作目录及本机 HTTP MCP 转发，启动 `claude --print`；Claude transcript 保存在用户的正常 Claude 配置目录中。子进程复用用户的 Claude 登录；不要求用户另建 API key。

未完成的任务保留 `state_dir` 中的会话映射和工作目录。A 保持 `run` 在线；核对任务、命令和 B journal 后，从 B 的 MCP 宿主调用 `tasks_resume`：

```json
{
  "task_id": "<task_id>",
  "idempotency_key": "resume-<new_uuid>"
}
```

工具返回原 `task_id` 和持久化的 `resume_request`。A 消费该显式请求后，使用原 Claude session UUID 和工作目录、新 assignment 及 relay 凭据恢复会话。`pending` 表示等待 A，`accepted` 表示请求已被领取；任务结果仍通过同一 ID 的 `tasks_get` / `tasks_wait` 查询。响应不明时复用同一个 key；本次恢复失败后核对原因，再用新 key 请求另一次尝试。unknown、reconcile 与隔离检查保持有效，缺失会话映射或 transcript 时不创建新会话。完整步骤见 [运维文档](docs/operations.md#恢复一个已保存的任务)。

自动记忆和普通 hooks 在子会话中关闭。用户及管理层的 Claude 规则仍可能适用；这些规则与前一个任务的聊天历史不同。该行为隔离任务上下文，不提供操作系统沙箱。参数、权限和恢复边界见 [运维步骤](docs/operations.md)。

可选的手工交互 channel 模式仍使用 `examples/claude-mcp.json`，它在多个任务之间保留同一会话。启用前先停止同一 agent 身份的控制器；准入和启动步骤见 [兼容性记录](docs/compatibility.md)。新控制器的 headless 子进程不加载 development channel。

B 工具包括 `tasks_submit`、`tasks_run`、`tasks_get`、`tasks_wait`、`tasks_resume`、`tasks_cancel`、`tasks_list` 和 `tasks_events`。控制器接收并领取分派后启动 Claude；子进程通过 relay 使用 `bash` 工具，再调用 `task_complete` 保存结果。Claude 的终端回复和进程退出码不代替服务端最终结果。完整工具字段及恢复语义见 [协议文档](docs/protocol.md)。

`tasks_submit` 和 `tasks_run` 可通过可选的 `model`（如 `opus`、`sonnet` 或完整模型 ID）及 `effort`（`low`、`medium`、`high`、`xhigh`、`max`）指定该任务的模型和思考强度。任务级模型覆盖 agent 的默认模型；省略参数时保留现有默认行为。选择会保存到任务，并在恢复原会话时沿用；实际可用范围取决于 A 的 Claude 版本和账号。

新提交的 `prompt` 必须使用英文：1–8192 字节，仅允许可打印 ASCII 与 TAB/LF/CR，并且至少包含一个 `A–Z` 或 `a–z`。这项规则同时适用于 `tasks_submit` 和 `tasks_run`。已有任务的相同 key、相同内容重试仍读取原记录；恢复旧任务不会重新校验或改写其原始 prompt。

## 行为边界

- 一台服务端进程配合本地 SQLite；一个 A 身份固定工作区，工作区串行处理任务，默认每个任务使用不同 Claude 会话。
- A 子进程未提交最终结果便退出时，控制器保留原会话映射和工作目录，断开旧连接后自动重连并等待 B 的 `tasks_resume`。重连及旧 interrupted 通知不会自动重跑任务；每个恢复请求最多领取一次。
- 等待请求结束或断线不等于任务结束。使用返回的 `task_id`、`command_id` 查询已保存的状态。
- 相同逻辑命令重试复用 `operation_key`。结果不明时先查询；更换 key 无法避免重复副作用。
- 无法确认是否启动或是否完成的命令进入 `unknown`，需要核对。任意 Bash 副作用不具备 exactly-once 保证。
- 恢复旧服务端快照后，先使用 `task-bridge-client reconcile` 核对保留的 journal，再由管理员处理未知结果和工作区隔离。
- B 的命令使用配置账号的操作系统权限；工作区路径约束不构成隔离不互信代码的沙箱。
- B 不提供 PTY、交互 stdin、持久 shell 或后台命令 daemon；broker 不提供多实例写入或公开 Streamable HTTP MCP。本机 HTTP MCP 仅用于 A 控制器与当前 Claude 子进程通信。

## 文档与交付

- [架构](docs/architecture.md)：组件、身份、状态和故障处理。
- [协议](docs/protocol.md)：工具、B 控制消息、幂等与输出。
- [运维](docs/operations.md)：本地运行、Compose、管理、备份与恢复。
- [兼容性](docs/compatibility.md)：验证状态、逐任务 Claude 验收、可选 channel smoke 和平台限制。

CI 定义 Windows/Linux 的测试、vet 和构建，另设 Linux race 与容器构建，运行记录见 [GitHub Actions](https://github.com/Zxilly/task-bridge/actions)。`.goreleaser.yaml` 提供本地归档配置，默认关闭远程 Release 发布。

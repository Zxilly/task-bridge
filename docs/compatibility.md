# 兼容性与验证记录

记录日期：2026-10-23。此文件区分代码实现、可自动化的模拟测试和需要真实账号/平台的验收。

## 版本

| 项目 | 当前配置 | 证据范围 |
| --- | --- | --- |
| Go | `1.26.0`，以 `go.mod` 为准 | 工程目标；本机测试结果另记 |
| MCP Go SDK | `v1.8.0` | `go.mod`/`go.sum` 锁定 |
| WebSocket | `github.com/coder/websocket v1.8.15` | 版本锁定，自定义 SDK transport |
| SQLite | `modernc.org/sqlite v1.61.0` | 版本锁定，纯 Go 驱动 |
| B 控制协议 | `1` | 项目内部 JSON 协议，非 MCP |
| Claude Code | 需按实际运行版本记录 | 原计划的本机静态检查不能替代当前 Go 实现的真实连接 |
| Caddy 模板 | `2.11.7-alpine` | 明确镜像 tag，未在目标主机部署验收 |

## 本次交付检查

| 检查 | 结果 | 范围 |
| --- | --- | --- |
| 示例 JSON | 通过 | 七份 JSON 均可解析；路径和凭据需按实际环境填写 |
| Compose 渲染 | 通过 | 注入随机测试管理 token 后执行 `docker compose --env-file deploy/env.example -f deploy/compose.yaml config --quiet`；模板 token 留空，未填写时拒绝运行 |
| Caddyfile | 通过 | 官方 Caddy 2.11.7 Windows amd64 二进制 `validate`；下载按官方 SHA512 校验 |
| GoReleaser 配置 | 通过 | 官方 GoReleaser 2.18.3 `check`；下载按官方 SHA256 校验 |
| CI YAML / actionlint | 通过 | YAML 解析和 actionlint 1.7.12 语义检查；未调用外部 shellcheck/pyflakes。远程运行记录见 [GitHub Actions](https://github.com/Zxilly/task-bridge/actions) |
| B doctor | 通过 | Windows amd64 / Go 1.26.0；示例换为临时工作区与测试凭据，未执行 shell、未联网 |
| Windows 测试 / race / vet | 全部包通过 | Go 1.26.0 windows/amd64，普通测试、`go test -race ./...`、`go vet ./...` 及 `go mod verify` |
| Linux 测试 | 全部包通过 | WSL Ubuntu / Go 1.26.3 linux/amd64，`go test -v -count=1 -timeout=10m ./...` |
| Linux vet / build | 通过 | `go vet ./...`；`CGO_ENABLED=0 go build -trimpath ./cmd/...` |
| Linux race | 全部包通过 | `go test -race -count=1 -timeout=10m ./...` |
| SQLite 备份与恢复测试 | 通过 | Windows Go 1.26.0 普通测试；WSL Go 1.26.3 race 测试，覆盖并发快照、凭据失效与原数据保留 |
| 真正编译的 B CLI stdio | Windows / Linux 通过 | 官方 Go SDK host 通过真实子进程 stdio 列出七个工具并完成任务；stdout/stderr 不混入协议，host EOF 停止 Bash、进程退出并释放 journal 锁 |
| 快照回滚与真实 B journal | Windows / Linux 通过 | 实际 SQLite 快照回滚，显式 reconcile 区分 1 known / 1 orphan；known 证据重放、orphan 不重建，B 工作区中两个文件的副作用都仅一次 |
| 5 A / 10 B 小规模模拟 | Windows / Linux 通过 | 真实 MCP WS A 协议客户端、B WS/独立 journal/SQLite/Bash；五个主任务完成，五个无 A 任务保持 queued，额外后继任务验证串行接棒 |
| 慢 WS 消费端断开与重放 | Windows / Linux race 通过 | 对端停止读取后被断开；100 条各约 32 KiB 的未 ACK outbox 消息保留，正常读取消费端重连后全部重放、逐 ID 确认并保持连接 |
| Docker build/run | 未执行 | 本机 Docker Desktop Linux engine 未运行 |
| 真实 Claude channel | 未执行 | 预检为 `loggedIn=false, authMethod=none, apiProvider=firstParty`，没有 Anthropic 环境凭据 |

自动化总测覆盖 broker、client、executor、MCP transport、HTTP 生命周期、storage 和 integration 包。A 仍是官方 SDK 协议测试客户端，具有真实 WS 连接并读取 channel 通知；它不代表真实 Claude 模型或账号。按用户确认，真实 Claude channel、目标生产部署/TLS 和实际拟用宿主的验收延期；P0/P6 对应外部验收仍未通过。

小规模模拟中，五个实际 Bash 命令各执行一次；同 key 重试返回同一 command，跨工作区的 A/B 查询被拒绝，同一任务的并发命令被拒绝，后继任务在前一任务 complete 后才分派。下面记录 Go 测试进程的离散采样，数值受 GC 和先前测试影响，不能当作 broker 独占内存、峰值或容量上限：

| 环境 | 该测试耗时 | HeapAlloc：开始 / 已连 5 A + 10 B / 结束，bytes |
| --- | --- | --- |
| Windows amd64，Go 1.26.0，独立运行该测试 | 3.944 秒 | 1,020,192 / 29,116,552 / 3,791,360 |
| WSL Ubuntu amd64，Go 1.26.3，整套测试中运行 | 1.141 秒 | 33,311,888 / 11,989,416 / 3,090,296 |

## 补充验证

- Windows 与 WSL Linux 均已通过真实 B CLI 进程强制结束后的恢复测试：保留 journal、报告 unknown、隔离工作区，原进程的文件副作用只发生一次。
- Windows 与 WSL Linux 均通过 401/403 凭据拒绝后停止重连的 race 回归。
- command_wait 的零值采用 30 秒默认等待，相关 broker/integration 测试已重新通过。
- broker/B 的逐 ID ACK 与连接内重传记录、B Hello 工作区子集路由，以及受限执行诊断的持久化/对账已有相应回归覆盖；正常非零退出仍为 exited。
- Windows/Linux amd64、arm64 的服务端与客户端均完成 CGO_ENABLED=0 交叉构建；amd64 二进制已在各自系统运行 --version。ARM64 尚无原生运行验证。

## 尚未通过的外部验收

- 真实 Claude Code → 远程 WS MCP → channel 唤起 → B Bash → Claude complete 的完整闭环。
- 目标 Claude 账号的 channel feature/组织准入、交互确认与实际权限行为。
- 独立 Linux 服务器上的原生运行；WSL Ubuntu 的完整 Go 测试与 race 已通过。
- 目标服务器 Docker/Compose、TLS/WSS、Caddy reload、停机与旧备份恢复演练。
- 实际拟用的第三方 B MCP 宿主中的完整工作流；官方 Go SDK host 已通过真实 CLI stdio 测试。
- Windows ARM64/Linux ARM64 原生运行、持续压力、上述具体回归以外的慢消费者/故障组合、磁盘故障演练和 RPO/RTO。

远程 Linux/Windows CI、Linux race 与容器构建的运行记录见 [GitHub Actions](https://github.com/Zxilly/task-bridge/actions)；本页的平台测试记录来自本地 Windows 与 WSL。交叉编译不等于目标系统运行测试。外部验收结果完成后，应追加版本、命令、时间和观察证据。

## 真实 Claude channel smoke

远程 WebSocket MCP 和 channel 的行为依据 [Claude MCP 文档](https://code.claude.com/docs/en/mcp#option-4-add-a-remote-websocket-server)、[Channels reference](https://code.claude.com/docs/en/channels-reference)及 [CLI reference](https://code.claude.com/docs/en/cli-reference#cli-flags)。预览期功能或组织策略可能使工具连接成功但 channel 被拒绝。

准备独立测试目录和可写的 B 工作区，用真实 Claude 版本记录以下信息：

```text
日期与系统：
Claude Code 版本：
账号/组织准入状态（不含凭据）：
MCP Go SDK 版本：
WebSocket mcp 子协议回显：
initialize 协商的 MCP 协议版本：
启动命令和交互确认：
内置工具列表与远程工具列表：
第一个任务 / 第二个任务：
Claude 忙碌通知 / A 重连：
B hostname、cwd、随机标记与文件证据：
最终 task_id 与查询结果：
结论与阻断原因：
```

配置使用 `examples/claude-mcp.json`，URL 和设备 token 与对应 agent/workspace 匹配。开发 channel 的启动形态需以该 Claude 版本的 `--help` 和真实确认流程复核：

```sh
claude --tools "" --strict-mcp-config --mcp-config agent-mcp.json \
  --dangerously-load-development-channels server:bridge
```

`--tools ""` 的目的是使测试只使用指定远程工具。`--allowedTools` 是审批规则，不作为内置工具禁用的替代。开发 channel 的交互准入不被假定为 headless 可用；组织拒绝时记录实际原因，不将轮询视作 channel 验证成功。

必须等待 initialize 和 initialized 完成后发送通知。原计划记录 MCP `2026-07-28` 与 channel 存在兼容限制，计划目标为 `2025-11-25`；Go SDK 的最终协商结果必须从真实会话记录，不能仅凭配置推断。

验收步骤：

1. B MCP 工具提交一个包含随机标记的任务，Claude 闲置时收到真实 channel。
2. Claude 接受分派，用远程 Bash 返回 hostname/cwd，并在 B 工作区创建含标记的文件。
3. Claude 提交结果，B 通过原等待或 task_id 查询得到相同持久结果。
4. 重复任务通知、第二个任务、忙碌时通知及重连不产生重复命令。
5. 验证 A 的本地执行工具没有参与，保留工具调用和文件证据。

## 平台与执行限制

Linux 使用已安装的 Bash。Windows 使用 Git for Windows Bash，配置路径指向真实 `bash.exe`。本地命令采用 `--noprofile --norc -c`，没有交互 stdin、PTY 或跨命令持久 shell；依赖 shell profile 的 PATH/环境需在 B 配置中明确提供。

Linux 进程组和 Windows 普通子孙进程树已分别通过超时终止测试；真实 B CLI 的宿主 EOF 也经过停止执行的验证。Windows 每次执行使用独立 Job Object，在 Bash 恢复运行前完成绑定；取消直接终止 Job，客户端异常退出也会关闭 Job 并停止其中的子进程。刻意脱离平台进程管理或保留继承输出管道的后台进程不在首版支持范围；无法确认的命令应保留 unknown 状态。恢复不依赖过期 PID 来杀进程，以免误伤 PID 复用后的其他进程。

cwd 检查覆盖配置工作区路径与符号链接解析，但不是操作系统沙箱。任意命令仍以 B 的账号权限运行。

## 旧快照恢复

恢复后服务端刷新 epoch 并禁用恢复出的设备凭据，B 的普通 MCP 连接发现 epoch 变化会停止自动连接并保留 journal。提供 `task-bridge-client reconcile --config client.json --reason "实际核对证据"` 进行显式核对：需停止原 B 并使用新 token，全部批次确认后原子归档证据并切换本地 epoch。已知终态可以核对采纳；unknown 原始证据和 orphan 记录保留审计。该命令不执行或确认 dispatch，不杀旧 PID，也不解除工作区隔离。

快照中原 queued 任务没有 assignment，恢复后采用管理员审查、abandon、release 后按需重新提交的保守流程。代码与自动化测试不能代替目标服务器上的完整旧快照恢复演练，不应据此宣称已验证 RPO/RTO。

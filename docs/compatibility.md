# 兼容性与验证记录

记录日期：2026-10-24。此文件区分代码实现、可自动化的模拟测试和需要真实账号/平台的验收。2026-10-23 与提交 `5ab3291` 的完整回归记录保留为基线；新增 B MCP 恢复入口的检查范围单列，不沿用基线的全量通过结论。

## 版本

| 项目 | 当前配置 | 证据范围 |
| --- | --- | --- |
| Go | `1.26.0`，以 `go.mod` 为准 | 工程目标；本机测试结果另记 |
| MCP Go SDK | `v1.8.0` | `go.mod`/`go.sum` 锁定 |
| WebSocket | `github.com/coder/websocket v1.8.15` | 版本锁定，自定义 SDK transport |
| SQLite | `modernc.org/sqlite v1.61.0` | 版本锁定，纯 Go 驱动 |
| B 控制协议 | `1` | 项目内部 JSON 协议，非 MCP |
| Claude Code | 本机静态检查为 `2.1.283` | 已检查 `--version`、`--help` 和 `auth login --help`；未调用模型，不代表账号登录或真实任务已验证 |
| Caddy 模板 | `2.11.7-alpine` | 明确镜像 tag，未在目标主机部署验收 |

## 已有基础实现检查

本节记录新增逐任务 A 控制器之前已完成的 server/B 验证。新的控制器、launcher 与 relay 检查范围单列在下节，不能由原 A 协议模拟客户端的结果替代。

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
| 真正编译的 B CLI stdio | Windows / Linux 通过 | 官方 Go SDK host 通过真实子进程 stdio 发现当时的工具集合并完成任务；stdout/stderr 不混入协议，host EOF 停止 Bash、进程退出并释放 journal 锁 |
| 快照回滚与真实 B journal | Windows / Linux 通过 | 实际 SQLite 快照回滚，显式 reconcile 区分 1 known / 1 orphan；known 证据重放、orphan 不重建，B 工作区中两个文件的副作用都仅一次 |
| 5 A / 10 B 小规模模拟 | Windows / Linux 通过 | 真实 MCP WS A 协议客户端、B WS/独立 journal/SQLite/Bash；五个主任务完成，五个无 A 任务保持 queued，额外后继任务验证串行接棒 |
| 慢 WS 消费端断开与重放 | Windows / Linux race 通过 | 对端停止读取后被断开；100 条各约 32 KiB 的未 ACK outbox 消息保留，正常读取消费端重连后全部重放、逐 ID 确认并保持连接 |
| Docker build/run | 未执行 | 本机 Docker Desktop Linux engine 未运行 |
| 真实 Claude channel | 未执行 | 预检为 `loggedIn=false, authMethod=none, apiProvider=firstParty`，没有 Anthropic 环境凭据 |

上述自动化总测覆盖 broker、client、executor、MCP transport、HTTP 生命周期、storage 和 integration 包。原 A 测试端是官方 SDK 协议客户端，具有真实 WS 连接并读取 channel 通知；它不代表真实 Claude 模型或账号。按用户确认，真实 Claude、目标生产部署/TLS 和实际拟用宿主的验收留待后续。

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

- 现有用户登录 → A 控制器 → 每任务新 Claude Code `-p` 进程 → 本机 HTTP MCP → B Bash → Claude complete 的真实模型闭环、连续两个任务的会话隔离，以及同一任务的原会话恢复。
- 可选手工 channel 模式的真实推送、目标账号准入、交互确认与权限行为；默认 worker 不要求 headless 子进程加载 development channel。
- 独立 Linux 服务器上的原生运行；WSL Ubuntu 的完整 Go 测试与 race 已通过。
- 目标服务器 Docker/Compose、TLS/WSS、Caddy reload、停机与旧备份恢复演练。
- 实际拟用的第三方 B MCP 宿主中的完整工作流；官方 Go SDK host 已通过真实 CLI stdio 测试。
- Windows ARM64/Linux ARM64 原生运行、持续压力、上述具体回归以外的慢消费者/故障组合、磁盘故障演练和 RPO/RTO。

远程 Linux/Windows CI、Linux race 与容器构建的运行记录见 [GitHub Actions](https://github.com/Zxilly/task-bridge/actions)；本页的平台测试记录来自本地 Windows 与 WSL。交叉编译不等于目标系统运行测试。外部验收结果完成后，应追加版本、命令、时间和观察证据。

## 逐任务 A 控制器验收

`task-bridge-agent` 将 A 的连接生命周期与 Claude 会话生命周期分开。控制器用官方 Go MCP SDK 接收 broker 通知，每个新任务创建不同的 session UUID 和 cwd，通过只服务该任务的本机 HTTP MCP 转发工具。B 通过 `tasks_resume` 请求恢复同一未完成任务的 UUID、cwd 和 Claude transcript；A 常驻领取请求，每次启动均创建新的 relay 配置和凭据。任务结果仍由 `task_complete` 提交；不把 CLI 的普通文本或成功退出当作任务完成。

以下检查记录于 2026-10-23，对应逐任务控制器的初始实现；本轮新增恢复逻辑及全量回归见后续的 2026-10-24 记录。

| 定向检查 | 结果 | 范围 |
| --- | --- | --- |
| A CLI doctor | 通过 | 使用可被路径检查发现但不能执行的假 Claude 文件；不启动进程、不创建 state_dir、不联网、不输出设备 token |
| 控制器与真实 broker | 通过 | 两个任务使用不同 session/cwd/token；前一个子会话退出后才启动后继；未完成即退出会断开上游；assignment 变化会停止旧会话 |
| Claude 进程启动器 | Windows / Linux 通过 | 真实 helper 子进程验证启动参数、stdin、环境、目录、非零退出诊断与取消后的进程树清理；不调用模型 |
| 控制器初始版本全量回归 | Windows / Linux 通过 | 2026-10-23 基线：全部包的普通测试、vet 和 CGO_ENABLED=0 构建；Linux 全部包 race 通过 |
| HTTP MCP relay | 通过 | 官方 SDK 客户端验证任务范围、原始 JSON、鉴权/Origin、响应大小、取消和关闭；定向 race 通过 |
| 示例与入口指南 | 通过 | 8 份 examples JSON、两指南的 6 个 JSON 示例均可解析；A/B smoke payload 一致 |
| GoReleaser 新 agent 配置 | 通过 | GoReleaser 2.18.3 `check`，平台归档包含 server、client、agent 三个 build ID |
| 真实 Claude 模型运行 | 未执行 | 仅检查本机 CLI 帮助；没有调用推理来代替用户完成登录或验收 |

2026-10-24、提交 `5ab3291` 的恢复与英文输入基线。当时的恢复入口是手动 A CLI，以下结果不能替代新增 B MCP 入口的验证：

| 定向检查 | 结果 | 范围 |
| --- | --- | --- |
| Claude 启动与恢复参数 | Windows / Linux 通过 | `TestCommandLauncher` 使用真实 helper 进程检查 fresh/resume 互斥参数、原 UUID/cwd、新 MCP 配置和凭据、环境、取消与失败诊断；缺失 transcript 的模拟错误只启动一次，不退回新会话 |
| 真实 broker 会话恢复 | Windows / Linux 通过 | 模拟 launcher 验证 interrupted 与未领取任务恢复原 session UUID/cwd、更新 assignment 和 relay token；unknown 命令、缺失映射或分派属于另一 agent 时不启动 Claude；终态仅清理保存状态 |
| 本地 session store | Windows / Linux 通过 | broker/task/workspace/agent 身份绑定、清单持久化和损坏拒绝、并发创建不覆盖、崩溃残留保留，以及仅删除已验证任务目录；包含符号链接边界检查 |
| 父进程强杀 | Windows / Linux 通过 | 真实 helper 进程验证父进程被强杀、无法执行 deferred cleanup 时受管 CLI 仍停止；覆盖普通退出、重复关闭和启动失败清理 |
| 英文 prompt 校验 | Windows / Linux 通过 | model、broker、client 包测试；覆盖字符/字节边界、拒绝时无任务或分派副作用、旧任务精确重试，以及 SDK → WebSocket → broker 调用 |
| `5ab3291` 基线全量回归 | Windows / Linux 通过 | 全部包 `go test -count=1`、`go vet`、全部命令 `CGO_ENABLED=0` 构建；Linux 全部包 race 通过；`go mod verify`、gofmt 和 diff 检查通过 |
| 真实 Claude transcript 恢复 | 未执行 | 未调用真实模型；helper 进程不验证 Claude 自身的 transcript 读写或恢复行为 |

新增 B MCP 恢复入口的检查：

| 检查 | 结果 | 范围 |
| --- | --- | --- |
| A MCP 恢复工具 schema | Windows / Linux 通过 | 官方 SDK 发现 11 个上游工具；两种 task_resume 形式与 controller-only task_resume_failed 的必需字段、长度边界、未知字段拒绝和鉴权路由 |
| 恢复 channel 线格式 | Windows / Linux 通过 | 实际 WebSocket 验证无请求 ID 时严格三个 metadata 字段，有请求 ID 时严格四个；支持空 assignment_id，并覆盖含恢复字段的 32 KiB 边界 |
| 示例与入口指南 | 静态通过 | 8 份 examples JSON、两指南的 8 个 JSON 示例均可解析；A/B smoke 一致，恢复示例保留原 task_id，本地文档链接有效 |
| B MCP → 常驻 A 恢复闭环 | Windows / Linux 通过 | 官方 B MCP SDK、真实 WebSocket、SQLite、Bash 与模拟 launcher；恢复原 task/UUID/cwd、新 assignment/token，同 key 不重新启动，新 key 明确再试；缺失映射记录 failed，A 继续在线 |
| 实际 CLI 入口 | 通过 | 本机构建的 A 提供 run/doctor 运维命令，resume 返回 unknown command；真实 B stdio 子进程测试发现八个工具并覆盖正常任务与宿主 EOF |
| 本轮全量回归 | Windows / Linux 通过 | 全部包 `go test -count=1 -timeout=10m ./...`、`go vet ./...`、全部命令 CGO_ENABLED=0 构建；Linux 全部包 race 通过；go mod verify、全部 Go 文件 gofmt 与 diff 检查通过 |
| 远程 CI | [见 Actions](https://github.com/Zxilly/task-bridge/actions/workflows/ci.yml) | Linux、Windows、Linux race 和容器构建以对应提交的 GitHub Actions 记录为准 |
| 真实 Claude 模型与 transcript | 未执行 | 不使用模拟 launcher、SDK 或 helper 结果代替真实 Claude 验收 |

2026-10-27 增加连接侧 `agents_usage` 额度查询。Windows 全量测试、vet、三程序构建，以及 A、broker、B 包 race 通过。真实 helper 子进程在持续运行时输出额度事件，A 控制器通过 WebSocket MCP 自动上报，B 的官方 MCP 客户端读到两个窗口；另覆盖无任务查询、单窗口、字段缺失、零用量、过期、工作区授权、旧连接隔离、超长行和 stderr 隔离。测试明确检查 Claude 子会话没有额度工具或处理提示。编译的 B CLI stdio 测试发现九个工具并查询到已上报快照。helper 不调用模型；本机 Claude 未登录，未验证真实订阅账号的事件字段或刷新频率。`unifiedWindows` 为上游观测字段，兼容性边界见 [账号额度协议](protocol.md#连接侧账号额度)。

2026-10-28 现场记录暴露了空闲边界：任务后的快照先因超过 5 分钟变为 stale，A 重连后又因没有新任务而一直返回 available=false。增加连接后立即执行、此后每分钟执行的 `/usage` 刷新，并保留任务事件补充。回归测试覆盖无任务查询、断开重连、定期刷新、临时失败保留旧值、过时事件拒绝覆盖，以及查询进程树取消。Windows 全量测试、vet、三程序构建和 A／broker／B／model 包 race 通过。VM 上 Claude Code 2.1.310 的真实 claude.ai Pro 登录已返回结构化 usage_report，num_turns、费用、token 消耗均为零；同时确认 --disable-slash-commands 会让该命令不可用，因此仅从独立额度查询中移除该参数。

本机静态检查确认 Claude Code 2.1.283 提供 `--print`、`--session-id`、`--resume`、`--tools`、`--allowedTools`、`--strict-mcp-config`、`--mcp-config`、`--permission-prompts`、`--output-format` 和 `--verbose`。这些参数与 [CLI reference](https://code.claude.com/docs/en/cli-reference#cli-flags) 对照过，但未使用真实账号执行付费推理。自动化测试可以检查会话参数、cwd、relay 授权、进程清理和 broker 状态；真实 Claude 是否保存、恢复上下文并按要求调用工具仍须单独验收。

默认子进程复用用户正常 Claude 登录，不使用会改变 OAuth 行为的 `--bare`。它保留 transcript 持久化，关闭 auto memory 和普通 hooks；新任务使用 `--session-id`，显式恢复同一任务使用 `--resume`，不使用 `--continue` 或 `--fork-session`。用户及 managed 规则仍可适用；managed hooks 不受普通会话设置的关闭覆盖。[会话恢复](https://code.claude.com/docs/en/sessions#what-a-resumed-session-restores)、[auto memory](https://code.claude.com/docs/en/settings-reference#automemoryenabled) 和 [hooks](https://code.claude.com/docs/en/settings-reference#disableallhooks) 说明了这些边界。

真实验收使用 `examples/agent-config.json`，在运行 A 的同一系统账号下保留或完成 `claude auth login`：

```sh
bin/task-bridge-agent doctor --config agent.json
bin/task-bridge-agent run --config agent.json
```

1. 从 B 提交带唯一 ASCII 标记的英文只读任务，检查 controller 收到分派，先完成 `task_accept` 再启动 Claude 子进程，Claude 首先读取 `task_get`。
2. Claude 通过桥接 Bash 返回 B 的 hostname、cwd 和标记，明确调用 `task_complete`；B 查询得到同一个持久结果。
3. 提交第二个独立任务，记录不同的 Claude session UUID、工作目录及第二个 task_id/result，确认第一个子进程已经结束。
4. 检查新任务使用 `--session-id`，MCP 配置只指向当前任务 relay，CLI 文本没有被当作最终结果。
5. 在另一个受控未完成任务中使 Claude 子进程退出，确认 session 映射与 cwd 保留、A 重连保持在线且不自动重跑。核对任务、命令和 B journal，排除 unknown 或待处理 reconcile 后，由 B MCP 调用 `tasks_resume`，参数为原 task_id 和新的 idempotency_key。
6. 记录 pending → accepted 的请求状态；确认 broker 先完成恢复授权，Claude 以原 UUID、原 cwd 和 `--resume` 继续原上下文，使用新的 assignment、relay 凭据和配置。B 仍查询同一个 task ID，并得到持久终态；accepted 本身不算验收成功。
7. 核对终态仅清理控制器持有的映射和任务目录，Claude profile 中 transcript 保留；映射或 transcript 缺失时请求失败，不创建新会话，A 仍在线。同 key 重试不能再次启动失败请求；核对原因后新 key 才表示另一次明确尝试。

记录日期、平台、二进制/Claude 版本、登录方式（不含凭据）、各 task_id/session_id、request_id 与请求状态、恢复前后 assignment、执行标记及终态。`doctor` 不启动 Claude 或连接 broker，因此只算本地配置检查，不算该验收通过。

## 可选手工交互 channel smoke

该模式直接在一个交互式 Claude 会话内接收多个任务，不提供默认 worker 的每任务新会话隔离。开始前停止同一 agent 身份的控制器。远程 WebSocket MCP 和 channel 的行为依据 [Claude MCP 文档](https://code.claude.com/docs/en/mcp#option-4-add-a-remote-websocket-server)、[Channels reference](https://code.claude.com/docs/en/channels-reference)及 [CLI reference](https://code.claude.com/docs/en/cli-reference#cli-flags)。预览期功能或组织策略可能使工具连接成功但 channel 被拒绝。

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

`--tools ""` 用于关闭本地执行和文件工具。`--allowedTools` 是审批规则，不作为内置工具禁用的替代。官方文档明确说明这个 development-channel 旗标在 `-p` 或 Agent SDK 中被忽略，不能用它实现默认 headless worker；组织拒绝时记录实际原因，不将轮询视作 channel 验证成功。

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

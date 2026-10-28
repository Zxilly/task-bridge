# 运行与维护

本项目按单个所有者、一个 broker 进程和本地 SQLite 部署。平台、逐任务 Claude 运行与可选手工 channel 的验证记录见 [compatibility.md](compatibility.md)。下列文件是可执行部署模板，仍需在目标 Linux 主机完成 HTTPS/WSS、代理重载和恢复演练。

## 本地运行

从源码构建 server、agent 和 client 三个应用，按 README 创建 `server.json`。服务端配置如下：

| 字段 | 含义 |
| --- | --- |
| `listen` | A/B 与健康检查入口；开发默认 `127.0.0.1:8080` |
| `database` | 本机 SQLite 文件；相对路径以配置文件目录为基准 |
| `admin_listen` | 管理 API；使用 loopback 地址 |
| `admin_token_env` | 保存管理 token 的环境变量名称 |
| `max_queued_tasks` | 最大排队任务数 |
| `max_database_bytes` | 数据库及 WAL 的分配字节上限，默认 2 GiB，至少 32 MiB |

```sh
bin/task-bridge-server serve --config server.json
bin/task-bridge-server healthcheck --url http://127.0.0.1:8080/readyz
```

`/healthz` 判断进程是否存活，`/readyz` 判断服务是否可以接收工作。监控使用独立探针，不将已有 WebSocket 连接数量当作 readiness。停止服务端会中断当前连接与等待请求；调用者以业务 ID 查询状态。

管理 CLI 读取同一份配置中的本机管理地址，并从 `admin_token_env` 读取凭据。不要通过反向代理公开管理端口。设备 token 只允许访问对应设备与工作区；管理 token 可执行恢复性操作，需要单独保存。

## B 配置与 stdio

`examples/client-config.json` 和 Windows 版本列出字段。`server_url` 指向 `/v1/client`。公网使用 `wss://`；非 loopback 的明文 `ws://` 需要显式 `allow_insecure_ws: true`，只用于受控开发网络。

`token_env` 与 `token_file` 二选一。`token_file` 相对配置文件目录解析，读取后去除两端空白；不要将 token 放入 host MCP 的可共享配置。MCP 宿主需要把所用的环境变量传入 B 进程。

`journal_path` 相对配置文件目录解析。它保存恢复与去重信息，不能在每次启动时清空。每个 workspace 的 `root` 设为真实项目目录，`bash_path` 指向 Bash 可执行文件，`env` 显式增加执行环境变量。普通命令只继承必要系统环境，不会继承 B token 等全部宿主变量。

Hello 只报告当前配置中的工作区。设备可以连接其授权工作区的子集；未配置工作区的任务保持 queued，新的 Bash 请求被拒绝。检查排队不前的任务时，除了 A/B 是否在线，也要核对 B 本次上报的工作区集合。

`max_output_bytes` 默认每条命令保存 10 MiB，最大可设 50 MiB。`max_journal_bytes` 默认 1 GiB，至少 32 MiB，按 journal 数据库及 WAL 分配字节计算。接近容量上限时拒绝新执行或停止继续收集输出，保留终态写入空间；已经记录的去重和未知结果不能通过清空 journal 来释放。

```sh
bin/task-bridge-client doctor --config client.json
bin/task-bridge-client doctor --connect --config client.json
bin/task-bridge-client mcp --config client.json
```

普通 doctor 检查本地配置和依赖；`--connect` 只主动探测 TCP/TLS 可达性，不发送认证握手或注册 executor，不能用它证明 token 已被服务端接受。两种模式都不打印 token。stdio 运行时 stdout 只包含 MCP JSON-RPC，日志位于 stderr。客户端生命周期跟随宿主；关闭宿主或 stdin EOF 会触发清理，不作为后台 daemon 使用。

Windows 的 `root` 使用 Windows 路径，`bash_path` 通常为 `C:\Program Files\Git\bin\bash.exe`。Go 配置中的路径按 Windows 规则解析；Bash 命令内部的路径由 Git Bash 解释。cwd 可使用 workspace 内相对目录。不要把 `/c/...` shell 路径直接当成 Go 的 Windows cwd 字段。

`tasks_submit` 和 `tasks_run` 的新任务 `prompt` 要求英文，长度为 1–8192 字节，只接受可打印 ASCII（0x20–0x7E）及 TAB、LF、CR，并至少包含一个英文字母。非法 prompt 返回 `invalid_arguments`，不会创建或分派任务。提交结果不明时仍使用原 key 和原内容重试；已有非英文任务的原样幂等重试不受新规则影响，不要为了重试翻译或改写原内容。

## A 控制器与 Claude 登录

默认 A 使用 `task-bridge-agent run`。控制器接收 broker 的 channel 通知，为每个新任务启动独立的 `claude --print` 子进程，并响应 B 发起的原会话恢复请求。Claude 通过当前任务的本机 HTTP MCP relay 调用远程工具，不直接在 headless 模式加载 development channel。

在运行控制器的同一个系统账号下安装 Claude Code，并完成 `claude auth login`。已有有效登录可以直接复用。不要复制认证文件或为了运行控制器切换为 API key。`--bare` 不使用订阅 OAuth，因此默认启动参数不使用它。

复制 `examples/agent-config.json`，填写：

| 字段 | 含义 |
| --- | --- |
| `server_url` | broker 的 `/mcp/agent` WS/WSS 地址；公网必须使用 WSS |
| `token_env` 或 `token_file` | `claude` 角色设备 token，二选一；与 Claude 账号登录分开 |
| `claude_path` | Claude CLI 名称或可执行文件路径，默认 `claude` |
| `state_dir` | 私有状态目录，默认配置目录下的 `agent-state`；持有排他锁，保存未完成任务的会话映射与工作目录 |
| `model` | 可选；任务未指定 `model` 时使用此默认值，均未填写时使用 Claude 的正常模型选择 |
| `allow_insecure_ws` | 仅为受控开发网络显式允许非 loopback 的明文 WS，默认关闭 |

相对 `state_dir` 和 `token_file` 以配置文件目录解析；含目录分隔符的相对 `claude_path` 也使用该基准。配置不做 `${VAR}` 字符串展开。不要将 token 提交到仓库或放在命令行参数中。

```sh
claude --version
claude auth login
bin/task-bridge-agent doctor --config agent.json
bin/task-bridge-agent run --config agent.json
```

省略 `run` 子命令也会启动控制器。`doctor` 不运行 Claude、不调用模型、不联网，只验证配置、可执行文件路径和设备凭据是否存在；不能用它证明 Claude 登录或设备鉴权已通过。控制器日志写入 stderr；`run --log-file /private/path/agent.log` 可同时将 JSON 日志追加到文件，父目录必须已存在。文件不会自动轮转。使用 Windows 任务计划程序时，让任务直接启动 agent，配置此参数保存日志，以便计划任务能跟踪并停止实际控制器进程。每个 A 身份只运行一个控制器，同一 `state_dir` 由文件锁排他使用；停止实例前先检查任务状态。

每个新任务使用不同的 session UUID 和工作目录，首次以 `--session-id` 启动 Claude。显式恢复同一任务时，使用保存的 UUID、原工作目录与 `--resume`；每次启动都生成新的本机 relay 凭据和配置，不使用 `--continue` 或 `--fork-session`。子会话禁用本地执行和文件工具，只加载本次 relay 的 MCP 配置。`task_progress` 与 `task_complete` 仍是回传进度和结果的接口，Claude 的 stdout 或退出码不替代 broker 里的最终结果。

连接侧 B 用 `agents_usage` 查询账号额度，例如 `{"workspace_id":"demo"}`，也可增加 `agent_id` 筛选。A 连接后立即通过独立的 Claude `/usage` 命令刷新，此后每分钟刷新一次，任务流式事件也会补充数据；Claude 任务子会话无需处理额度。无需新增配置，需更新 server、A 和 B 二进制，并让 MCP 宿主重新发现 B 的工具。B 查询只读取 broker 内存，不创建任务或模型回合；控制器的本地命令使用现有登录查询账号。A 断线后不列出，重连或 broker 重启后立即重新刷新；初次刷新可能需要几秒。检查可用数据时，应覆盖无任务的空闲 A、重连后和超过 5 分钟后的查询。缺失数据、超过 5 分钟或已到重置时间的窗口均不能当作当前剩余额度。字段、时间单位和 CLI 支持边界见 [账号额度协议](protocol.md#连接侧账号额度)。

子进程设置 `CLAUDE_CODE_DISABLE_AUTO_MEMORY=1`，并通过会话设置关闭 auto memory 和普通 hooks，避免跨任务自动记忆。managed hooks 只能由 managed 设置关闭；用户或 managed CLAUDE 规则仍可能进入上下文。auto memory 与会话 transcript 分开：transcript 保持启用，以便恢复同一任务。启动器清除继承的 transcript 禁写变量，并关闭 `CLAUDE_CODE_RESUME_INTERRUPTED_TURN`，让恢复使用本次明确的提示。工作目录约束不构成操作系统沙箱。官方说明见 [会话恢复](https://code.claude.com/docs/en/sessions#what-a-resumed-session-restores)、[autoMemoryEnabled](https://code.claude.com/docs/en/settings-reference#automemoryenabled) 与 [disableAllHooks](https://code.claude.com/docs/en/settings-reference#disableallhooks)。

`state_dir/sessions/` 保存任务与 Claude UUID 的映射和工作目录，不保存 Claude 登录凭据；MCP 配置每次启动时重建，进程结束后移除。未完成任务在进程退出后保留映射和工作目录；broker 确认终态后，控制器仅清理自己持有的任务目录。Claude 的 transcript 仍由 Claude 管理，默认位于 `~/.claude/projects/`，Windows 为 `%USERPROFILE%\.claude\projects\`，设置 `CLAUDE_CONFIG_DIR` 时使用对应位置。恢复时保留原系统账号与 Claude 配置目录。transcript 包含对话和工具结果，默认受 Claude 的 30 天保留期影响；本程序不会删除或复制用户 profile 中的 transcript。[Claude 会话存储说明](https://code.claude.com/docs/en/sessions#where-transcripts-are-stored)

### 恢复一个已保存的任务

子进程未完成任务便退出或处理任务时连接中断，控制器会断开旧上游连接，再自动重连等待显式请求。没有 `resume_request_id` 的 interrupted 通知只用于保持待恢复状态，不会启动 Claude。保留原 `agent.json`、`state_dir` 和 Claude profile，让同一 A 身份的控制器保持 `run` 在线。先检查原 task/command、B journal 与进程证据，再由最初提交任务的 B 凭据通过 MCP 调用 `tasks_resume`：

```json
{
  "task_id": "<task_id>",
  "idempotency_key": "resume-<new_uuid>"
}
```

响应包含原 `task_id` 与 `resume_request`。`pending` 表示请求已保存，原 A 离线时保持待领取；`accepted` 表示 A 已原子领取本次请求并获得新 assignment，不表示 Claude 已启动或任务已完成。A 使用原 UUID/cwd 和新的 relay 配置、凭据继续对话，任务结果仍从同一 ID 的 `tasks_get` / `tasks_wait` 读取。任务完成后 A 继续等待后续任务，不需要手动切换 CLI 模式。

同一次请求重试使用同一个 `idempotency_key`，即使已有更新的恢复尝试，也返回该 key 对应的历史 receipt。`failed` 的请求不会因重连再次执行；核对 `resume_request.error` 并处理原因后，用新 key 明确请求另一次尝试。已有 pending/accepted 请求时不能叠加新尝试。新请求要求原 task 仍可恢复、B 在线并已完成核对；running/offered、终态、期限已过、未知命令或工作区隔离会阻止新请求，恢复不会延长原 deadline。

缺失、损坏或身份不匹配的本地会话映射会使恢复请求失败；Claude 找不到原 transcript 时也直接失败，不使用新 UUID 兜底，控制器继续在线等待。旧版未保存 transcript 的任务不能凭 task ID 重建原上下文。不要删除状态文件、更换命令 key 或重新提交任务来绕过未知结果。`tasks_resume` 不接收新 prompt；已存任务的原始 prompt 不因新英文提交规则被改写或拒绝。

## Compose 与 Caddy

需要 Docker Engine、Compose plugin、一个指向服务器的域名以及可用的 80/443 端口。应用镜像由本地源码构建，模板没有依赖一个预先发布的镜像。

在项目根目录：

```sh
cp deploy/env.example deploy/.env
# 编辑 deploy/.env，填写域名、邮箱以及独立随机管理 token。
docker compose --env-file deploy/.env -f deploy/compose.yaml config --quiet
docker compose --env-file deploy/.env -f deploy/compose.yaml up --build -d
docker compose --env-file deploy/.env -f deploy/compose.yaml ps
```

如果已用旧模板部署，在后续 Compose 命令中使用 `docker compose -p <existing-project> ...`，将 `<existing-project>` 替换为原 project 名称，以复用已有命名卷。产品改名本身不迁移数据库或 Caddy 证书。

Compose 的 shell 环境可能覆盖 `.env` 中的同名变量；部署前检查实际环境。不要把包含凭据的完整 `docker compose config` 输出复制到日志或工单。[Docker 的环境变量优先级](https://docs.docker.com/compose/how-tos/environment-variables/envvars-precedence/)解释了加载规则。

broker 使用 scratch 镜像和 UID/GID `65532:65532`，根文件系统只读，数据库写入 `bridge_data` 命名卷。首次创建的卷由镜像中的目录初始化；复用已有卷时需要核对所有权。只向 Compose 网络暴露 8080，管理地址仍为容器 loopback。

Caddy 公开 80/443 并保存证书到 `caddy_data`。Caddyfile 仅代理健康检查、两个 WS 入口和 `/setup/claude.md`、`/setup/client.md` 配置指南，其他路径返回 404。指南公开且不包含设备凭据，可将对应的完整 URL 交给 A 或 B 的 agent 配置本端。无需手写 Upgrade/Connection 转发头；Caddy 的 reverse proxy 支持 WebSocket。模板的 `stream_close_delay 5m` 仅延迟 reload 导致的连接关闭，A/B 仍必须支持重连。[Caddy streaming 文档](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#streaming)说明该行为。

```sh
curl --fail https://bridge.example.com/readyz
curl --fail https://bridge.example.com/setup/claude.md
curl --fail https://bridge.example.com/setup/client.md
docker compose --env-file deploy/.env -f deploy/compose.yaml exec caddy \
  caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
docker compose --env-file deploy/.env -f deploy/compose.yaml exec caddy \
  caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
```

模板固定 Go 与 Caddy tag；生产部署验证完成后，记录本地 broker 镜像 ID，并可将 Caddy 镜像改为经验证的 digest。不要在同一个 SQLite 卷上启动多个 broker，也不要把 SQLite 或 B journal 放在共享网络文件系统上。

## 设备与恢复管理

以下命令由管理凭据授权。容器部署时，将 `bin/task-bridge-server admin --config server.json` 替换为 `docker compose --env-file deploy/.env -f deploy/compose.yaml exec broker /task-bridge-server admin --config /etc/bridge/server.json`。

```sh
bin/task-bridge-server admin --config server.json workspaces create --id demo --executor laptop
bin/task-bridge-server admin --config server.json devices create --kind executor --executor laptop --workspace demo
bin/task-bridge-server admin --config server.json devices create --kind claude --agent claude-demo --workspace demo
bin/task-bridge-server admin --config server.json devices revoke PRINCIPAL_ID
bin/task-bridge-server admin --config server.json tasks inspect TASK_ID
```

执行不明的任务先检查服务端命令记录、B journal 和现场进程。保留证据后使用 `commands resolve` 记录核对结论，或通过 `tasks abandon` 结束追踪。`workspaces release` 是独立动作，只有确认旧进程已结束或环境已重新隔离后执行。使用 `--help` 查看当前命令接受的 outcome 与必需理由，不把 unknown 直接当成成功。

吊销 token 后检查已有连接已关闭，旧 token 的重连被拒绝。旧快照恢复会使此前的吊销记录回滚，因此恢复流程需要废止恢复出的全部设备 token。

### 更换遗失或退役的 B journal

服务端持久绑定 executor 与 journal 的 stream ID。普通 MCP 连接若报告不同 journal，会返回 `journal_mismatch` 并停止自动重连。原 journal 仍在时，使用原文件；服务端快照回滚的证据核对使用下文的 `task-bridge-client reconcile`。

只有在 B 已离线、管理员已核实旧执行停止，且该 executor 没有未决或 unknown 命令时，才使用管理命令重置 journal 绑定：

```sh
bin/task-bridge-server admin --config server.json executors reset-journal laptop --reason "填写已核实旧执行全部停止的证据"
bin/task-bridge-server admin --config server.json devices create --kind executor --executor laptop --workspace demo
```

reset-journal 清除旧 stream 绑定、吊销该 executor 的全部旧 B token，并保存审计理由；之后使用新 token 和新 journal 路径重新接入。它不解除工作区隔离，也不把 unknown 当成未执行。保留尚可取得的旧 journal 和核对证据。

reconcile 的用途是核对保留的原 journal 并跨服务端 epoch 迁移证据；reset-journal 的用途是在明确停止、没有未决执行后授权换用另一个 journal。两者不能互相替代。

## 备份、升级与恢复

使用 broker 的一致备份命令。运行时只复制主 `.db` 文件会漏掉 WAL 中的已提交事务。备份文件包含任务内容、命令输出与凭据哈希，应存放在受保护路径，并复制到另一磁盘或主机。

实现使用 SQLite `VACUUM INTO` 生成一致快照；`--output` 是 broker 本机路径，必须是尚不存在的新文件，不能覆盖正在运行的数据库或既有备份。

```sh
bin/task-bridge-server admin --config server.json backup --output /absolute/path/to/bridge-backup.db
```

容器内的可写路径在 `/data`。执行备份后用 `docker compose cp` 将文件导出，后续按日期或版本保存，避免下一次备份覆盖唯一可恢复副本。

升级前完成备份，停止 broker，替换二进制或重建镜像，运行迁移并重新启动：

```sh
bin/task-bridge-server migrate --config server.json
bin/task-bridge-server serve --config server.json
```

迁移使用数据库锁，要求没有另一个服务端写同一数据库。不提供自动 down migration；回退旧程序前先确认数据库版本兼容，必要时从已验证的备份恢复。

恢复需要停止 broker，并保留当前数据库、WAL/SHM 与 B journal。复制一份配置为 `server.restore.json`，把其中 `database` 改为新路径，例如 `./restored/bridge.db`。目标数据库文件必须尚不存在；restore 不覆盖现有数据库：

```sh
bin/task-bridge-server restore --config server.restore.json --input /absolute/path/to/bridge-backup.db
bin/task-bridge-server serve --config server.restore.json
```

restore 先将备份复制到临时文件并执行 integrity check，刷新 server epoch，禁用恢复出的全部设备 token，将所有未完成任务（包含 queued）置为 interrupted、隔离对应工作区，将未决命令置为 unknown，并清除旧 outbox/游标，再将文件移动到尚不存在的新目标。需要时让服务配置指向这个已恢复路径；不要手动覆盖旧库。备份中的 queued 任务不会自动再次排队。

重新签发设备凭据后，普通 B MCP 连接检测到 epoch 变化会停止自动连接并保留 journal。旧快照恢复的核对步骤如下：

1. 停止 B MCP 进程，核实原有 Bash/子孙进程的现场状态，保留原 journal 及输出；不要按陈旧 PID 自动杀进程。
2. 为同一个 executor/workspace 签发有效的新设备 token，更新 B 的凭据来源。保持 journal 路径指向原有文件。
3. 带实际核对理由运行显式 reconcile。它必须独占 journal 的 flock，全部服务端分批确认成功后，才在本地事务中归档旧 pending/unknown 证据并切换 epoch。
4. 重新启动 MCP，已匹配 command 的证据补传给 broker；服务端已知的明确终态可以在显式对账中被本地采用，原 unknown 证据仍保留归档。快照中不存在的 orphan 命令只保留审计证据，不凭空重建或执行。
5. 管理员检查仍然 unknown 的命令与隔离工作区，必要时记录 resolution/abandon，再单独 release。对账本身不解除隔离。

```sh
bin/task-bridge-client reconcile --config client.json --reason "填写已经核实的恢复证据"
bin/task-bridge-client mcp --config client.json
```

reconcile 不执行命令，不确认派发，也不自动重跑旧任务。中途失败时保留原 epoch 与待发送记录，检查原因后用同一 journal 重试。恢复后“服务端没有命令记录”不能证明 B 没有执行过。

快照中原来 queued 的任务没有 assignment，恢复为 interrupted 后不能通过 A 的 `task_resume` 直接恢复。核对并审查后，管理员将旧任务 abandon，确认现场已安全后 release 工作区，再按需要提交新任务和新幂等 key。本版不自动 requeue 这些任务。

常规进程重启与旧快照恢复使用不同语义，不通过手动复制数据库绕过恢复命令。记录演练时的备份时间、恢复耗时、丢失区间与核对结果；本版没有实测 RPO/RTO 或零数据丢失承诺。

## 部署验收

在实际目标环境完成以下检查后再记为部署通过：

- A/B 分别通过 WSS 认证，A 接受真实 channel 通知并调用远程工具。
- 用 hostname、随机标记和项目内文件证明命令在 B 执行。
- 等待任务时能并发执行命令；重复同一 key 不再次产生执行效果。
- Caddy reload 和 broker 重启后可查询原任务；不盲目重启已派发命令。
- B 断网、进程超时、取消、输出截断以及未知结果均有可观察状态。
- 在空目录恢复备份并核对 epoch、凭据失效、journal 与工作区隔离。

broker 在 `max_database_bytes` 内预留 16 MiB 用于已有命令终态；数据库加 WAL 达到预留边界，或所在磁盘可用空间少于 32 MiB 时，拒绝新任务与新命令。这个准入限制不能保证磁盘在外部写入或故障后仍有空间。使用监控提前处理容量，持续压力和目标环境下的故障组合仍需实测。Compose 的日志轮转只限制容器日志，不替代数据库/journal 的容量治理。

已有慢消费者回归覆盖一个具体场景：WS B 对端在握手后停止读取，broker 将其断开，100 条各约 32 KiB 的未确认 outbox 消息仍持久保留；正常读取方重连后全部重放、逐 ID 确认并保持连接。该场景已在 Windows/Linux race 下通过。两端的连接内已发送集合避免部分 ACK 引发整批重放；未 ACK 消息至少间隔 5 秒重传，重连立即重放。这不代表所有网络限速、持续压力和故障组合均已验收。

当前输出保留策略：broker 每小时分批清理已知终态命令且任务已结案、结束超过 7 天的输出，并反映到 truncated/dropped_bytes；任务、命令幂等记录与 unknown 证据保留。B 在确认服务端 epoch 后，只清理已由服务端确认终态超过 72 小时的本地输出；未确认、unknown、orphan 和对账归档证据不按普通输出策略删除。数据库文件可能复用空闲页而不立即缩小，不能只按文件尺寸判断清理是否生效。

Prometheus 指标位于 `admin_listen` 的 `/metrics`，使用同一管理 Bearer 凭据，默认不向公网开放。指标包括在线 A/B、任务状态数、未决/unknown 命令、待确认 outbox、数据库加 WAL 字节，以及标准 Go/进程指标。容量准入会考虑可复用的 SQLite freelist 页；文件尺寸与净在用数据量不同。监控抓取方应从受保护的凭据配置加载管理 token。

## 本地归档

`.goreleaser.yaml` 使用 GoReleaser 2.18.3 配置语法，归档包含两个应用、示例与部署文档，目标为 Linux/Windows amd64 和 arm64。配置关闭了远程 Release 发布。源码放入 Git 仓库并准备好版本元数据后，可以生成 snapshot：

```sh
goreleaser check
goreleaser release --snapshot --clean --skip=publish
```

二进制归档使用其中的两个可执行文件直接运行。归档附带的 Dockerfile/Compose 是参考模板，执行 `up --build` 需要完整源码目录中的 `go.mod`、`cmd/` 和 `internal/`，不能只对二进制归档执行源码构建步骤。

生成归档不是目标平台原生运行验证；ARM64 支持范围仍需实际机器测试。不要用机器上较旧的 GoReleaser 2.0.1 读取这个配置。

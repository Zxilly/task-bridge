# 运行与维护

本项目按单个所有者、一个 broker 进程和本地 SQLite 部署。平台与真实 Claude channel 的验证记录见 [compatibility.md](compatibility.md)。下列文件是可执行部署模板，仍需在目标 Linux 主机完成 HTTPS/WSS、代理重载和恢复演练。

## 本地运行

从源码构建两个应用，按 README 创建 `server.json`。服务端配置如下：

| 字段 | 含义 |
| --- | --- |
| `listen` | A/B 与健康检查入口；开发默认 `127.0.0.1:8080` |
| `database` | 本机 SQLite 文件；相对路径以配置文件目录为基准 |
| `admin_listen` | 管理 API；使用 loopback 地址 |
| `admin_token_env` | 保存管理 token 的环境变量名称 |
| `max_queued_tasks` | 最大排队任务数 |
| `max_database_bytes` | 数据库及 WAL 的分配字节上限，默认 2 GiB，至少 32 MiB |

```sh
bin/bridge-server serve --config server.json
bin/bridge-server healthcheck --url http://127.0.0.1:8080/readyz
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
bin/bridge-client doctor --config client.json
bin/bridge-client doctor --connect --config client.json
bin/bridge-client mcp --config client.json
```

普通 doctor 检查本地配置和依赖；`--connect` 只主动探测 TCP/TLS 可达性，不发送认证握手或注册 executor，不能用它证明 token 已被服务端接受。两种模式都不打印 token。stdio 运行时 stdout 只包含 MCP JSON-RPC，日志位于 stderr。客户端生命周期跟随宿主；关闭宿主或 stdin EOF 会触发清理，不作为后台 daemon 使用。

Windows 的 `root` 使用 Windows 路径，`bash_path` 通常为 `C:\Program Files\Git\bin\bash.exe`。Go 配置中的路径按 Windows 规则解析；Bash 命令内部的路径由 Git Bash 解释。cwd 可使用 workspace 内相对目录。不要把 `/c/...` shell 路径直接当成 Go 的 Windows cwd 字段。

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

Compose 的 shell 环境可能覆盖 `.env` 中的同名变量；部署前检查实际环境。不要把包含凭据的完整 `docker compose config` 输出复制到日志或工单。[Docker 的环境变量优先级](https://docs.docker.com/compose/how-tos/environment-variables/envvars-precedence/)解释了加载规则。

broker 使用 scratch 镜像和 UID/GID `65532:65532`，根文件系统只读，数据库写入 `bridge_data` 命名卷。首次创建的卷由镜像中的目录初始化；复用已有卷时需要核对所有权。只向 Compose 网络暴露 8080，管理地址仍为容器 loopback。

Caddy 公开 80/443 并保存证书到 `caddy_data`。Caddyfile 仅代理健康检查与两个 WS 入口，其他路径返回 404。无需手写 Upgrade/Connection 转发头；Caddy 的 reverse proxy 支持 WebSocket。模板的 `stream_close_delay 5m` 仅延迟 reload 导致的连接关闭，A/B 仍必须支持重连。[Caddy streaming 文档](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#streaming)说明该行为。

```sh
curl --fail https://bridge.example.com/readyz
docker compose --env-file deploy/.env -f deploy/compose.yaml exec caddy \
  caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile
docker compose --env-file deploy/.env -f deploy/compose.yaml exec caddy \
  caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile
```

模板固定 Go 与 Caddy tag；生产部署验证完成后，记录本地 broker 镜像 ID，并可将 Caddy 镜像改为经验证的 digest。不要在同一个 SQLite 卷上启动多个 broker，也不要把 SQLite 或 B journal 放在共享网络文件系统上。

## 设备与恢复管理

以下命令由管理凭据授权。容器部署时，将 `bin/bridge-server admin --config server.json` 替换为 `docker compose --env-file deploy/.env -f deploy/compose.yaml exec broker /bridge-server admin --config /etc/bridge/server.json`。

```sh
bin/bridge-server admin --config server.json workspaces create --id demo --executor laptop
bin/bridge-server admin --config server.json devices create --kind executor --executor laptop --workspace demo
bin/bridge-server admin --config server.json devices create --kind claude --agent claude-demo --workspace demo
bin/bridge-server admin --config server.json devices revoke PRINCIPAL_ID
bin/bridge-server admin --config server.json tasks inspect TASK_ID
```

执行不明的任务先检查服务端命令记录、B journal 和现场进程。保留证据后使用 `commands resolve` 记录核对结论，或通过 `tasks abandon` 结束追踪。`workspaces release` 是独立动作，只有确认旧进程已结束或环境已重新隔离后执行。使用 `--help` 查看当前命令接受的 outcome 与必需理由，不把 unknown 直接当成成功。

吊销 token 后检查已有连接已关闭，旧 token 的重连被拒绝。旧快照恢复会使此前的吊销记录回滚，因此恢复流程需要废止恢复出的全部设备 token。

### 更换遗失或退役的 B journal

服务端持久绑定 executor 与 journal 的 stream ID。普通 MCP 连接若报告不同 journal，会返回 `journal_mismatch` 并停止自动重连。原 journal 仍在时，使用原文件；服务端快照回滚的证据核对使用下文的 `bridge-client reconcile`。

只有在 B 已离线、管理员已核实旧执行停止，且该 executor 没有未决或 unknown 命令时，才使用管理命令重置 journal 绑定：

```sh
bin/bridge-server admin --config server.json executors reset-journal laptop --reason "填写已核实旧执行全部停止的证据"
bin/bridge-server admin --config server.json devices create --kind executor --executor laptop --workspace demo
```

reset-journal 清除旧 stream 绑定、吊销该 executor 的全部旧 B token，并保存审计理由；之后使用新 token 和新 journal 路径重新接入。它不解除工作区隔离，也不把 unknown 当成未执行。保留尚可取得的旧 journal 和核对证据。

reconcile 的用途是核对保留的原 journal 并跨服务端 epoch 迁移证据；reset-journal 的用途是在明确停止、没有未决执行后授权换用另一个 journal。两者不能互相替代。

## 备份、升级与恢复

使用 broker 的一致备份命令。运行时只复制主 `.db` 文件会漏掉 WAL 中的已提交事务。备份文件包含任务内容、命令输出与凭据哈希，应存放在受保护路径，并复制到另一磁盘或主机。

实现使用 SQLite `VACUUM INTO` 生成一致快照；`--output` 是 broker 本机路径，必须是尚不存在的新文件，不能覆盖正在运行的数据库或既有备份。

```sh
bin/bridge-server admin --config server.json backup --output /absolute/path/to/bridge-backup.db
```

容器内的可写路径在 `/data`。执行备份后用 `docker compose cp` 将文件导出，后续按日期或版本保存，避免下一次备份覆盖唯一可恢复副本。

升级前完成备份，停止 broker，替换二进制或重建镜像，运行迁移并重新启动：

```sh
bin/bridge-server migrate --config server.json
bin/bridge-server serve --config server.json
```

迁移使用数据库锁，要求没有另一个服务端写同一数据库。不提供自动 down migration；回退旧程序前先确认数据库版本兼容，必要时从已验证的备份恢复。

恢复需要停止 broker，并保留当前数据库、WAL/SHM 与 B journal。复制一份配置为 `server.restore.json`，把其中 `database` 改为新路径，例如 `./restored/bridge.db`。目标数据库文件必须尚不存在；restore 不覆盖现有数据库：

```sh
bin/bridge-server restore --config server.restore.json --input /absolute/path/to/bridge-backup.db
bin/bridge-server serve --config server.restore.json
```

restore 先将备份复制到临时文件并执行 integrity check，刷新 server epoch，禁用恢复出的全部设备 token，将所有未完成任务（包含 queued）置为 interrupted、隔离对应工作区，将未决命令置为 unknown，并清除旧 outbox/游标，再将文件移动到尚不存在的新目标。需要时让服务配置指向这个已恢复路径；不要手动覆盖旧库。备份中的 queued 任务不会自动再次排队。

重新签发设备凭据后，普通 B MCP 连接检测到 epoch 变化会停止自动连接并保留 journal。旧快照恢复的核对步骤如下：

1. 停止 B MCP 进程，核实原有 Bash/子孙进程的现场状态，保留原 journal 及输出；不要按陈旧 PID 自动杀进程。
2. 为同一个 executor/workspace 签发有效的新设备 token，更新 B 的凭据来源。保持 journal 路径指向原有文件。
3. 带实际核对理由运行显式 reconcile。它必须独占 journal 的 flock，全部服务端分批确认成功后，才在本地事务中归档旧 pending/unknown 证据并切换 epoch。
4. 重新启动 MCP，已匹配 command 的证据补传给 broker；服务端已知的明确终态可以在显式对账中被本地采用，原 unknown 证据仍保留归档。快照中不存在的 orphan 命令只保留审计证据，不凭空重建或执行。
5. 管理员检查仍然 unknown 的命令与隔离工作区，必要时记录 resolution/abandon，再单独 release。对账本身不解除隔离。

```sh
bin/bridge-client reconcile --config client.json --reason "填写已经核实的恢复证据"
bin/bridge-client mcp --config client.json
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

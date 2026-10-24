# 协议

字段使用 snake_case。时间间隔单位为毫秒，持久时间戳为 UTC Unix 毫秒。稳定的 task/command ID 与连接 ID 分开使用。数据结构由 `internal/model` 定义，业务授权由 broker 校验。

## A：WebSocket MCP

入口为 `/mcp/agent`，在 WebSocket upgrade 前验证 `Authorization: Bearer <A token>`。客户端必须提供 `mcp` 子协议，服务端回显 `mcp`。每条连接建立独立的官方 MCP SDK server/session。

当前 transport 将 MCP 协议锁定为 `2025-11-25`，并声明：

```json
{
  "tools": {},
  "experimental": {
    "claude/channel": {}
  }
}
```

只有完成 initialize 和 initialized 后才将 agent 注册到调度器。工具 schema 与通知都通过官方 SDK 的会话使用。真实 Claude 是否接受 channel 仍需实际账号验收。

channel 示例：

```json
{
  "jsonrpc": "2.0",
  "method": "notifications/claude/channel",
  "params": {
    "content": "New task. Call task_accept ...",
    "meta": {
      "task_id": "task-id",
      "assignment_id": "assignment-id",
      "workspace_id": "demo"
    }
  }
}
```

| 工具 | 必需参数 | 可选参数与行为 |
| --- | --- | --- |
| `task_accept` | `task_id, assignment_id` | 接受 offered 分派；同一有效分派可重复接受 |
| `task_get` | `task_id` | 返回任务与最近命令快照 |
| `task_resume` | `task_id` | 控制器提供 `resume_request_id` 领取 B 的恢复请求；兼容手工 MCP 提供 `previous_assignment_id`，有 pending 请求时禁止绕过。两种 ID 必须严格二选一，均最多 128 字节 |
| `task_resume_failed` | `task_id, resume_request_id, error_code, message` | 仅供控制器记录恢复失败，返回 TaskResumeResult；error_code 最多 64 字节、message 最多 1024 UTF-8 字节，不向子会话 relay 暴露 |
| `bash` | `task_id, assignment_id, operation_key, command` | `cwd, timeout_ms, wait_ms`；命令固定在该任务的 B 工作区执行 |
| `command_get` | `task_id, command_id` | 返回命令元数据及首段输出 |
| `command_wait` | `task_id, command_id` | `wait_ms`；有界等待，任务执行期限不变 |
| `command_output` | `task_id, command_id` | `cursor, limit`；分页读取输出 |
| `command_cancel` | `task_id, command_id` | `reason`；显式请求停止，允许处理旧分派的遗留命令 |
| `task_progress` | `task_id, assignment_id, message` | 保存进度文字 |
| `task_complete` | `task_id, assignment_id, outcome, summary` | `result`；outcome 为 succeeded 或 failed |

A 的执行、进度与完成请求都要求当前连接和有效 assignment。历史查询不延续旧执行授权。不能仅凭“当前任务”推断请求属于哪个任务。

恢复通知在 `meta` 中增加可选字符串 `resume_request_id`。尚无 assignment 的旧 queued 任务可带空 `assignment_id` 和非空请求 ID，实际 assignment 在领取请求时签发。默认控制器只在收到匹配的持久恢复请求后读取本地 Claude session 映射，通过 `task_resume` 原子领取请求，再使用原 Claude UUID、原 cwd 和本次新 relay 配置恢复对话。无请求 ID 的 interrupted 通知不会启动 Claude。unknown 命令、未完成的 reconcile 或工作区隔离仍阻止恢复；缺失映射或 transcript 时上报 `task_resume_failed`，不创建新会话兜底。该工具记录请求失败，不提交任务终态；诊断使用受限的公开错误信息，不上传 CLI 原始日志。

`bash` 默认执行超时 120 秒，上限 15 分钟，且受任务剩余期限限制。默认等待 30 秒；`wait_ms` 上限 120 秒。等待结束后 `pending` 表示命令是否尚未结束，后续通过 command ID 查询。取消 MCP 请求只结束该次等待；业务停止使用 cancel 工具。

命令结果包含 `command` 对象以及 `command_id`、`stdout`、`stderr`、`chunks`、`text_omitted`、`cursor`、`has_more`、`truncated`、`dropped_bytes`、`pending`。`command` 中有 state、exit_code、signal、duration_ms 和实际 cwd。执行无法启动或结果不明时，`command.error` 保存结构化诊断，`code` 最多 64 字节、`message` 最多 2048 字节；诊断随 command.finished 持久化，并在 reconcile 中保留和回传。正常启动后以非零退出码结束的命令仍为 `exited`，保留退出码与输出，不能当作网络错误。

`command_output` 的 cursor 是累计输出字节偏移，`limit` 参数范围 1–16384，默认 16384；实际单页最多读取 8192 字节，为 base64、JSON 转义和元数据留出空间，遇到多个短 chunk 时也可能返回更小页面。MCP 完整工具结果上限 32 KiB，计入 JSON 转义和包装；需要时请求更小分页。

每个 `chunks` 元素含 `chunk_seq, stream, offset, data`，其中 `data` 是原始字节的 base64。UTF-8 跨页或输出为非文本时，按 stream 解码并拼接这些原始字节，不能直接拼接展示字符串。只有页面中该 stream 的字节构成完整 UTF-8 且展示大小符合上限时才返回 `stdout`/`stderr` 文本，否则留空并设 `text_omitted: true`；原始字节仍在 `chunks` 中。

所有工具返回一个包含 JSON 的 MCP TextContent；业务错误设置 `isError: true` 并包含：

```json
{
  "error": {
    "code": "stale_assignment",
    "message": "current connection and active task assignment required",
    "retryable": false
  }
}
```

## B：stdio MCP

B 的 stdio 工具由官方 MCP Go SDK 托管。B 到 broker 的控制连接独立运行，等待任务期间仍可接收执行请求。

| 工具 | 必需参数 | 可选参数与行为 |
| --- | --- | --- |
| `tasks_submit` | `workspace_id, prompt, idempotency_key` | `deadline_ms`；持久化成功后返回 task_id |
| `tasks_run` | 同 submit | `wait_ms`；提交后执行一次有界等待 |
| `tasks_get` | `task_id` | 任务与最近命令快照 |
| `tasks_wait` | `task_id` | `wait_ms`；超时返回 pending 与快照 |
| `tasks_resume` | `task_id, idempotency_key` | 显式请求恢复原任务；返回原 task_id 与持久 resume_request，不等待任务完成 |
| `tasks_cancel` | `task_id` | `reason`；区分取消申请和停止确认 |
| `tasks_list` | 无 | `workspace_id, cursor, limit`；只列授权范围 |
| `tasks_events` | `task_id` | `after_seq, limit`；读取进度事件 |

任务 deadline 默认为 30 分钟，可设 1 秒至 24 小时。B 等待默认 25 秒，上限 60 秒。等待响应包含 `pending`、`task_id` 和 `snapshot`。网络断开时提交结果可能不明，使用相同 idempotency key重新提交找回原任务。

新任务 `prompt` 必须使用英文。`tasks_submit` 和 `tasks_run` 共用以下校验：1–8192 字节，仅允许 0x20–0x7E 的可打印 ASCII 以及 TAB（0x09）、LF（0x0A）、CR（0x0D），并至少包含一个 `A–Z` 或 `a–z`。非 ASCII 字符、其他控制字节或不含英文字母的内容返回 `invalid_arguments`，不创建任务、事件、命令或分派。这是确定性的字符约束，不执行自然语言识别。

幂等重试先核对已存记录：相同 key、相同请求仍返回原任务，即使旧 prompt 不符合新的英文规则；相同 key、不同请求返回 `idempotency_conflict`。已存任务的查询和恢复不重验或改写原 prompt。该输入规则只约束新提交的任务描述，不改变命令输出或已有任务结果的编码。

`tasks_resume` 只接受最初提交任务的 executor 凭据，并要求该 B 当前报告原工作区。`task_id` 最多 128 字节，`idempotency_key` 最多 200 字节；不接收新 prompt，不创建新 task，也不延长 deadline。新请求限于保留原 agent 的 interrupted 或 queued 任务；running/offered、终态、期限已过、其他活动分派、未核对命令或隔离工作区会被拒绝。

返回形态为 `{"task_id":"...","resume_request":{...}}`。request 包含 `request_id, task_id, requester_id, idempotency_key, state, agent_id, created_at, updated_at`，按状态附带 `previous_assignment_id, assignment_id, connection_id, accepted_at, error`；Task 的 `resume_request` 字段提供最新请求摘要。

| resume_request.state | 含义 |
| --- | --- |
| `pending` | 已持久化，等待原 A 领取；A 离线时保留，重连后再次交付 |
| `accepted` | 已原子领取并获得新 assignment；不代表 Claude 已启动或任务已完成，继续用 tasks_get/tasks_wait 查询原 task |
| `failed` | 本次请求已结束，error 说明原因；不自动再消费，处理原因后由 B 用新 key 请求另一次尝试 |

同一 B 凭据下相同 key 的重试读取原 receipt，已有新请求也不会覆盖旧 key 的结果；将同一 key 用于不同 task 返回 `idempotency_conflict`。已有 pending/accepted 请求时，新 key 返回 `resume_pending`。重复 channel、SDK 响应丢失或 A 重连不重复领取 accepted 请求。最终任务结果仍由 `task_complete` 定义，不能把恢复请求的 accepted 当作 succeeded。

列表默认每页 20 条，最多 50 条。cursor 是本实现的数字分页位置，列表变化时不提供固定快照分页保证。事件 `seq` 与列表 cursor 不可互换。

## B：控制 WebSocket

入口为 `/v1/client`。使用独立 executor token，在升级前认证。它是本项目的 JSON text-frame 协议，不是 MCP server-to-client tool 调用。

envelope：

```json
{
  "version": 1,
  "type": "rpc",
  "message_id": "durable-message-id",
  "request_id": "request-id",
  "server_epoch": "database-generation",
  "stream_id": "durable-stream-id",
  "seq": 1,
  "connection_generation": 1,
  "payload": {},
  "error": {
    "code": "example",
    "message": "example error",
    "retryable": false
  }
}
```

示例列出可选字段，不表示每类消息都携带 error/message_id。最大消息 1 MiB。`task_id`/`command_id` 位于相应 payload，而不是公共 envelope。

| 消息 | 方向 | 用途 |
| --- | --- | --- |
| `hello` / `welcome` | B→server / server→B | executor、工作区、客户端版本与恢复代次 |
| `rpc` / `response` | B→server / server→B | MCP 工具对应业务方法、`command_authorize`、显式 `executor_reconcile` 及结果 |
| `command.dispatch` | server→B | 固定 command ID、workspace、assignment、有效期与期限 |
| `command.accepted` | B→server | journal 已接收派发 |
| `command.started` | B→server | 进程已启动 |
| `command.output` | B→server | 有序 stdout/stderr 字节 chunk |
| `command.finished` | B→server | 退出、取消、超时、unknown 或未启动 |
| `command.cancel` | server→B | 保存取消 tombstone，再停止活动命令 |
| `ack` | 双向 | 指定持久消息已处理 |
| `error` / `server.draining` | server→B | 协议错误、停止接收新工作 |

hello 的 payload：

```json
{
  "executor_id": "laptop",
  "client_version": "0.1.0-dev",
  "workspaces": ["demo"],
  "stream_id": "b-journal-stream-id"
}
```

`workspaces` 是该 B 本次连接实际配置的工作区子集，服务端核对它们均归属该 executor 且在凭据范围内。调度与新命令授权同时检查这个集合；即使设备在线，未上报工作区的任务仍保持 queued，该工作区的新 `bash` 返回 `executor_unavailable`，不会被改成执行失败。

welcome 返回 `server_epoch`、server stream ID 与递增的 `connection_generation`。正常重连保留 journal stream；旧连接 generation 不能继续提交结果。已有健康连接时重复 executor 连接被拒绝。`peer_cursors` 当前用于持久绑定 executor 与 B journal stream，`last_ack` 不充当累计 ACK 游标。替换成不同 journal 返回 `journal_mismatch`；原文件遗失时，只有管理员核实停止且无未决/unknown 命令后，才可通过 `executors reset-journal` 吊销旧凭据、清绑定并签发新凭据。

RPC 用 `request_id` 对应响应，payload 为 `{"method":"tasks_get","args":{"task_id":"..."}}`。RPC 等待者属于当前连接；断线后重新查询业务 ID，不重放旧 response。

## 持久 ACK 与重放

当前 Go 实现按 `message_id` 逐条确认，ACK 的 `request_id` 指向被确认的消息。`seq` 用于持久流排序与诊断，不使用“最大 seq 表示累计确认”的规则。取消会优先于普通派发重传，因此接收顺序可以不同于原始 seq。

broker 在业务状态与 outbox 同一事务提交后发送。B 接收派发、取消时先持久化 journal 再 ACK；B 的命令事件保存在 pending 队列，收到 broker 的持久 ACK 后移除。重复消息不重复应用，重复 command ID 不重新启动进程。

broker 和 B 都为每条连接维护已发送 ID 与发送时间。ACK 只移除对应 ID 的持久待确认记录和连接内已发送标记，不触发其他未确认消息整体重放。同一连接中，未确认消息距上次发送至少 5 秒后才在后续发送轮次重传；新连接使用空的已发送集合，立即重放仍未确认的消息。

当前保留完整去重记录，不实现以过期累计游标恢复的 `resync_required` 流程。未来加入保留清理时，需要同时定义快照核对与重放边界，不能只删去重表。

输出 chunk 的 `data` 是 Go `[]byte` 的 JSON base64，`chunk_seq` 在同一 command 内单调递增。stdout/stderr 保留来源；不同 pipe 的物理并发输出不承诺原始全序。

## 幂等、取消与未知结果

任务按 requester + idempotency key去重，命令按 task + operation key去重。请求 hash 包含对应业务内容：相同 key 与相同请求读取原记录，内容变化返回 `idempotency_conflict`。任务幂等范围不跨设备凭据迁移，命令记录不会因为 MCP 等待超时而删除。

B 在真正启动前调用 `command_authorize`，核验当前 assignment、租约、deadline、取消状态和 agent 连接。迟到或取消过的派发保存为 not_started，不自动换 key 再执行。

本地 journal 中 accepted/starting/running 的记录在 B 重启后无法证明原进程结果，恢复为 unknown。PID 仅用于诊断，恢复时不按旧 PID 杀进程。unknown 会阻止自动 resume 和 task_complete，需要管理核对；结案与解除工作区隔离分开。

任务取消先保存 cancel_requested 并撤回新执行授权，再派发停止请求。取消 tombstone 在 B 落盘，阻止迟到 dispatch 启动。B 失联时无法宣称进程已停止。所有命令确定结束后才形成 cancelled/timed_out。

普通 broker 重启不变更 server epoch；恢复旧备份刷新 epoch 并废止设备 token。所有未完成任务（包括备份中的 queued）进入 interrupted，工作区隔离，未决命令进入 unknown，旧 outbox/游标清除。B 的普通 MCP 连接发现 epoch 变化会停止自动连接并保留原 journal，必须先核对，不能通过删除 journal 绕过未知执行记录。

显式 `task-bridge-client reconcile` 独占 journal，使用有效 executor 凭据通过 `executor_reconcile` 分批交换证据。全部批次确认后，本地原子归档旧 pending/unknown 信息、保留输出并切换 epoch，下一次 MCP 连接补传匹配命令的证据。服务端返回的明确终态可在显式核对时采用，同时归档原 unknown 证据。服务端没有记录的 orphan 仅保留审计，不自动重建派发；工作区仍需单独审查解除隔离。此连接不执行或 ACK dispatch。

旧 queued 任务缺少 assignment，核对后采用管理员 abandon、release、按需重新提交的流程，不自动 requeue。完整运维步骤与实际验证范围见 [operations.md](operations.md) 与 [compatibility.md](compatibility.md)。

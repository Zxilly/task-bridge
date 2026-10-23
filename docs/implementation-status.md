# Go 实现状态

本轮交付为 `0.1.0-dev`。服务端、B 客户端、管理 CLI、协议适配与测试全部使用 Go。完整计划见 [PLAN.md](../PLAN.md)；本页记录当前实现，实际账号和部署验收另列。

## 已实现

- A：认证后的远程 WebSocket MCP、官方 SDK 协议协商、channel 通知、任务领取/恢复、远程 Bash、命令查询/分页/取消、进度与完成工具。
- B：官方 SDK stdio MCP、任务提交/查询/等待/取消/列表/事件、独立 WebSocket 连接、Linux Bash 与 Windows Git Bash 执行器。
- 持久化：SQLite WAL/FULL、Goose 迁移、任务/命令幂等、逐消息 ACK、连接内已发送集合、未确认消息至少间隔 5 秒重传与重连立即重放、连接代次与 assignment fencing。
- 执行：受限目录解析、独立进程、stdout/stderr 字节流、超时、普通进程树取消、输出配额、有界等待和每连接并发限制。
- 路由与诊断：按 B Hello 实际上报的工作区子集分派和授权；未配置工作区保持排队。命令执行诊断限码 64 字节、消息 2048 字节，持久化并随 reconcile 回传；正常非零退出仍为 exited。
- 恢复：B 进程崩溃转 unknown；不会按旧 PID 杀进程或自动再执行。管理员可以带证据结案、解除隔离，或在旧 journal 确已丢失时撤销旧凭据后重置绑定。
- 备份：在线一致性快照；恢复到新路径并保留原数据；更换 epoch、废止恢复出的设备凭据、隔离全部未完成任务；B 显式 reconcile 保留原证据并对账。
- 运维：独立 loopback 管理 API、设备吊销、health/readiness、内部 Prometheus 指标、磁盘与数据库容量保护、输出保留、优雅退出、Docker/Compose/Caddy 模板与 CI。

使用的第三方库包括官方 MCP Go SDK、coder/websocket、modernc SQLite、Goose、Cobra、gofrs/flock、google/uuid、jsonschema-go 和 Prometheus Go client；系统接口与其余基础功能使用 Go 标准库和 golang.org/x/sys。

## 自动化验证

Windows amd64 和 WSL Ubuntu Linux amd64 均通过全量测试、race、go vet 和构建。具体版本、命令和范围见 [compatibility.md](compatibility.md)。

测试覆盖：

- 真实 SQLite + WebSocket + 官方 MCP SDK + Bash 的提交、channel 通知、文件修改、检查与完成流程。
- 实际编译的 B CLI 与官方 stdio 宿主互通；关闭 stdin 终止本地执行并保留结果。
- 强制杀死 B 进程后，重启将原执行标为 unknown，工作区隔离，原进程副作用只出现一次，不能自动恢复重跑。
- 旧快照回滚后，B journal 区分已知命令和 orphan，补回结果与输出；恢复前后的文件副作用保持一次。
- 同 key 幂等/冲突、跨工作区拒绝、旧 assignment 拒绝、取消与 deadline、UTF-8/二进制分页、超过 4096 chunks、结果限额、journal 容量、凭据与 journal 绑定。
- 显式 reconcile 中途失败和最终事务失败保持旧 epoch/证据；已知服务端终态的采纳保留原 unknown 历史。
- 5 个 A 协议客户端与 10 个 B 的小规模联调，工作区串行执行，无 A 的任务保持 queued。
- 慢 WS 消费端停止读取后被断开，100 条未确认消息保留，重连读取后全部重放并逐条确认，正常读取消费端保持连接；Windows/Linux race 均通过。其他压力与故障组合另行验收。

这里的 A 是协议测试客户端；它验证服务端和执行器，不代替真实 Claude 的账号准入与模型行为。

## 保留的验收项

- 按用户选择，真实 Claude Code channel 唤醒与完整任务验收待后续登录后完成。
- 目标服务器上的 Docker/TLS/WSS、Caddy reload 和实际部署演练尚未完成；本机仅验证配置，Docker engine 未运行。
- 用户实际拟用的 B MCP 宿主仍需现场验收；已验证官方 SDK 宿主与实际 stdio CLI。
- ARM64 交叉构建不代表 ARM64 原生运行通过。
- 持续容量压测、生产 RPO/RTO、所有数据库故障和慢消费者组合仍需按目标环境继续验证。

源码仓库为 [Zxilly/task-bridge](https://github.com/Zxilly/task-bridge)。当前尚未发布 GitHub Release 或容器镜像；本地开发版已提供源码与平台二进制包、部署配置和后续验收步骤。

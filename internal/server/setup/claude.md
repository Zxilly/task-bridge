# Configure Task Bridge agent A

Use these instructions when the user asks you to configure the Claude side of Task Bridge. The default is `task-bridge-agent`: a controller that connects to the broker and starts a fresh Claude Code process for each task. Claude uses a local HTTP MCP relay to request commands on B and submit the result. A does not need `task-bridge-client`; satisfy the installed Claude Code version's own platform prerequisites.

The other side follows [Configure client B](client.md). This public document contains no credentials and cannot enroll a device or workspace.

## Determine the broker address

Take the origin of the **final HTTP(S) URL from which this document was fetched**, retaining its explicit port and discarding the `/setup/claude.md` path, query, and fragment. Only use an origin belonging to the broker the user intended to configure; investigate an unexpected redirect before sending credentials there.

- `<BROKER_ORIGIN>` is that HTTP(S) origin.
- `<AGENT_WS_URL>` is the same origin with `https` changed to `wss`, or `http` changed to `ws`, followed by `/mcp/agent`.
- For example, `https://bridge.example.com/setup/claude.md` gives `wss://bridge.example.com/mcp/agent`.

Use a URL parser. Do not infer an internal proxy hostname or port from HTTP headers. If this document was copied into chat or opened from disk and its fetch URL is unknown, request the broker's public origin. Remote deployments require WSS. An intentional loopback development endpoint may use WS; do not enable `allow_insecure_ws` or disable certificate checks to bypass a connection failure.

## Inspect the installation and Claude login

Identify the operating system, current account, installed `task-bridge-agent` and `claude` executables, existing agent configuration, and running controllers. Reuse an existing correct setup. Run the installed executables' read-only checks:

```text
task-bridge-agent --version
task-bridge-agent --help
claude --version
claude --help
claude auth status
```

Report versions and whether Claude authentication is usable without printing credentials or full configuration files. The worker runs under the same OS account as the user's Claude installation and preserves its normal login. If login is needed, let the user complete the interactive sign-in:

```text
claude auth login
```

Do not replace an existing subscription login with API credentials, copy authentication files, or run a paid prompt merely to check the installation. The worker does not use `--bare`: that mode changes credential handling and does not use the normal subscription login. See [Claude's headless documentation](https://code.claude.com/docs/en/headless#start-faster-with-bare-mode).

The installed CLI must support `--print`, `--session-id`, `--no-session-persistence`, `--tools`, `--allowedTools`, `--strict-mcp-config`, `--mcp-config`, and `--permission-prompts`. Check the actual `--help`; do not claim that a version number alone proves a live run works.

Prefer an existing agent binary from a trusted installation or platform package. If a source build is needed, use a verified checkout of [Zxilly/task-bridge](https://github.com/Zxilly/task-bridge), preserve existing files, and record its revision. From its root:

```sh
go mod download
go build -trimpath -o bin/task-bridge-agent ./cmd/task-bridge-agent
```

Use `.exe` for the output on Windows. Read `go.mod` for the required Go version. Check actual release assets before assuming a published download name.

## Obtain the role token and prepare configuration

You need an operator-issued token of kind `claude`, bound to the intended agent and workspace, plus confirmation that B serves that workspace. Claude account authentication and this device token are separate. Reuse an existing correct token source; request only missing inputs. Do not invent IDs, use an executor token, request the admin token, or call the management API.

Choose a private persistent configuration directory outside source control. Preserve existing files and make a recoverable backup before a focused merge. Store the device token in a private file with owner-only permissions on Unix or an appropriately restricted ACL on Windows. Use secure input rather than placing the token in chat, command arguments, shell history, or generated documentation.

Create or update `agent.json` with this shape, replacing all angle-bracket placeholders:

```json
{
  "server_url": "<AGENT_WS_URL>",
  "token_file": "<ABSOLUTE_PRIVATE_AGENT_TOKEN_FILE>",
  "claude_path": "<ABSOLUTE_CLAUDE_EXECUTABLE>",
  "state_dir": "<ABSOLUTE_PRIVATE_AGENT_STATE_DIRECTORY>"
}
```

Use a JSON serializer so Windows paths are escaped correctly. `claude_path` may be `claude` if it resolves reliably in the controller's PATH. `state_dir` holds the controller's exclusive lock and per-task temporary directories. Use a private location that is not the B project directory or a shared scratch folder. The controller cleans up a task's temporary working directory and MCP configuration when that session ends.

If the launching process already inherits the private `BRIDGE_AGENT_TOKEN` variable, replace `token_file` with `"token_env": "BRIDGE_AGENT_TOKEN"`. Configure exactly one token source. The JSON does not expand `${VARIABLE}` expressions. Relative token-file and state-directory paths resolve against the directory containing `agent.json`; a relative executable path containing a path separator resolves there too. An optional `model` field overrides Claude's normal model selection for these tasks; omit it unless requested.

## Validate and start the controller

Run the actual binary with the absolute configuration path:

```sh
"/absolute/path/task-bridge-agent" doctor --config "/absolute/path/agent.json"
"/absolute/path/task-bridge-agent" run --config "/absolute/path/agent.json"
```

PowerShell requires the call operator before a quoted executable path:

```powershell
& 'C:\Private\TaskBridge\task-bridge-agent.exe' doctor --config 'C:\Private\TaskBridge\agent.json'
& 'C:\Private\TaskBridge\task-bridge-agent.exe' run --config 'C:\Private\TaskBridge\agent.json'
```

`doctor` validates the local configuration, executable path, and presence of the device credential. It does not start Claude, verify the Claude login, connect to the broker, or call a model. A successful doctor result is not an authenticated connection test.

Keep `run` alive in the user's intended terminal or service manager. It receives broker channel notifications itself through the MCP SDK. Do not start an extra interactive Claude channel session for this default mode, and do not put the controller into a stdio MCP host. One agent identity should have one active controller; inspect current work before stopping an existing instance. Diagnostics go to stderr.

## Understand each task's session

For each assigned task, the controller creates a fresh session UUID, a separate temporary working directory, and a local HTTP MCP relay restricted to that task. It launches `claude --print` with the task prompt, without `--resume` or `--continue`, and disables transcript persistence. The relay forwards permitted tool calls over the controller's broker connection. Work on the project still runs on B.

The child uses only the specified MCP configuration and has local execution and file tools disabled. Automatic memory is disabled for the child, and its settings disable ordinary hooks. Managed policy can still require managed hooks. Existing user or managed Claude instructions and the user's login may still apply; those shared rules are distinct from a previous task's conversation. This isolates task conversations, not the operating system or all account-level configuration. See [session behavior](https://code.claude.com/docs/en/agent-sdk/sessions), [auto memory](https://code.claude.com/docs/en/settings-reference#automemoryenabled), and [hook settings](https://code.claude.com/docs/en/settings-reference#disableallhooks).

Claude first calls `task_accept` through the relay, then uses bridge tools to execute commands, report progress, and complete the task. Its terminal text and a successful process exit do not substitute for the broker's durable final result. If the child exits without completing its task, the controller reports an error and disconnects. It does not automatically retry the task or resume an interrupted assignment. Preserve the task ID, inspect broker and B journal evidence, and use the operator's explicit recovery procedure. Do not create a new command key to escape an unknown outcome.

## Verify two independent tasks

Keep B's host-owned client running. Through B, call `tasks_submit` with real values and a new key:

```json
{
  "workspace_id": "<WORKSPACE_ID>",
  "idempotency_key": "setup-smoke-<NEW_UUID>",
  "deadline_ms": 300000,
  "prompt": "Use the bridge bash tool on B to run: printf 'task-bridge-smoke-<NONCE>\\n'; hostname; pwd. Do not write files. Wait for the command's known terminal result, read its output, and complete the task with the marker, hostname, and working directory in the summary."
}
```

Save the returned `task_id`. If a response is lost, retry the identical request with the same idempotency key. Use `tasks_wait` or `tasks_get` to confirm the durable final state and the expected B marker, hostname, and directory. A queued task, wait timeout, local process exit, or tool list alone does not prove success.

Submit a second task with a different key and nonce. Verify that the controller starts another Claude process with a different session ID and temporary directory, then returns that second task's durable result. Do not rely on asking the model whether it remembers the previous task as the sole isolation check. If B is unavailable, report that end-to-end verification is pending rather than acquiring B's token.

## Optional: manually operated interactive channel

The previous manual workflow remains available for an interactive Claude session. It keeps one conversation across tasks and does not provide the default worker's per-task session isolation. Stop the controller for that agent identity before using it. An operator can also use this mode to inspect interrupted state and explicitly call `task_resume` after reviewing execution evidence.

Configure the remote WebSocket endpoint in a dedicated MCP file:

```json
{
  "mcpServers": {
    "bridge": {
      "type": "ws",
      "url": "<AGENT_WS_URL>",
      "headers": {
        "Authorization": "Bearer ${BRIDGE_AGENT_TOKEN}"
      },
      "timeout": 150000
    }
  }
}
```

Keep the environment reference in this MCP JSON, and ensure the process inherits the separate A device token. Launch interactively:

```sh
claude --tools "" --strict-mcp-config --mcp-config "/absolute/path/agent-mcp.json" \
  --dangerously-load-development-channels server:bridge
```

The user handles the channel and MCP trust prompts. Verify `/mcp`, channel registration, and an unsolicited offer while Claude is idle. This custom development-channel flag is ignored with `-p` and through the Agent SDK, so it is not part of the default worker launch. Organization policy still applies. See [WebSocket MCP](https://code.claude.com/docs/en/mcp#option-4-add-a-remote-websocket-server) and [development channels](https://code.claude.com/docs/en/channels-reference#test-during-the-research-preview). Protocol tests do not prove live channel admission for a particular account.

## Diagnose and report

- Device authentication failure: check the endpoint, token source and `claude` role; ask the operator about scope or revocation. Never try the admin token.
- Claude process failure: check the configured executable, its version, normal account login, and the actual diagnostic. Do not switch authentication or permissions silently.
- Task stays queued: check that the controller and the matching B are online and B reports the workspace. Preserve the task ID and idempotency key.
- Interrupted task or unknown command: preserve controller state and the B journal, inspect broker records, and use the operator's recovery procedure.

Report changed configuration paths, binary and Claude versions, the nonsecret endpoint, secret source location without its contents, doctor scope, controller connection status, and both smoke task IDs/results. Distinguish configuration prepared, authenticated controller connected, and real per-task Claude execution verified. State any remaining blocker.

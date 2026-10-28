# Configure Task Bridge client B

Use these instructions when the user asks you to configure a local MCP host to submit tasks to Task Bridge and execute the resulting Bash commands on this machine. The host starts `task-bridge-client` as a stdio MCP subprocess. That process also maintains an outbound control WebSocket to the broker. It is not the Claude-facing WebSocket MCP endpoint.

Agent A follows [Configure Task Bridge agent A](claude.md). Its default controller starts a separate Claude Code session for each task. This public document provides no credentials and cannot register an executor, workspace, or agent.

## Determine the broker address

Take the origin of the **final HTTP(S) URL from which this document was fetched**, retaining its explicit port and discarding the `/setup/client.md` path, query, and fragment. Only use an origin belonging to the broker the user intended to configure; investigate an unexpected redirect before sending credentials there.

- `<BROKER_ORIGIN>` is that HTTP(S) origin.
- `<CLIENT_WS_URL>` is the same origin with `https` changed to `wss`, or `http` changed to `ws`, followed by `/v1/client`.
- For example, `https://bridge.example.com/setup/client.md` gives `wss://bridge.example.com/v1/client`.

Use a URL parser. Do not infer a proxy's internal hostname or port. If the document was copied or opened from disk and its fetch URL is unknown, request the broker's public origin. Remote deployments require WSS. A loopback development endpoint may use WS; do not enable `allow_insecure_ws` or disable TLS verification just to bypass a connection failure.

## Inspect before changing configuration

Identify the operating system and architecture, current MCP host and version, existing host configuration, available `task-bridge-client` binary, Bash executable, and intended workspace directory. Inspect existing bridge configuration and journal locations before selecting new paths. Reuse a working setup rather than starting a competing process.

You need these values from existing configuration or the operator:

1. The registered executor ID and logical workspace ID.
2. A token of kind `executor`, authorized for that executor and workspace.
3. The existing local directory that should receive command execution.
4. The MCP host to configure, if it cannot be identified from the user's request or current environment.

Request only missing necessary values. Do not invent registered IDs, use a `claude` device token, request the broker's admin token, or attempt enrollment through a public URL. The operator creates devices and workspace ownership separately. A local directory mapping does not register a server-side workspace.

## Locate or build the executable

Prefer an existing binary from the user's trusted installation or supplied platform package. Resolve its absolute path and run:

```text
task-bridge-client --version
task-bridge-client --help
```

Use that executable's actual path, with `.exe` on Windows. Check the repository's actual release assets before relying on a published download; do not invent a `releases/latest/download` URL or asset name.

If no suitable binary exists and source building is appropriate, use an existing verified checkout of [Zxilly/task-bridge](https://github.com/Zxilly/task-bridge), or clone it into a new dedicated directory. Preserve existing files and record the checked-out revision. Read `go.mod` for the required toolchain; this version requires Go 1.26. From the repository root:

```sh
go mod download
go build -trimpath -o bin/task-bridge-client ./cmd/task-bridge-client
```

On Windows, use `-o bin/task-bridge-client.exe`. Do not assume a package manager entry or download name that the project does not provide. Windows and Linux have automated coverage; verify the chosen executable on the actual machine rather than treating cross-compilation as a runtime check.

Find Bash explicitly. On Linux, an existing `/bin/bash` is a common choice. On Windows, use the installed Git for Windows Bash, commonly `C:\Program Files\Git\bin\bash.exe`; verify the actual path instead of assuming it exists. Do not use PowerShell, `cmd.exe`, or a WSL launcher as `bash_path`. Check that the selected executable runs `--version` before saving its path.

## Store the role token and client configuration

Reuse an existing secure token source. Prefer a private token file for desktop hosts whose environment inheritance is unclear. Store only the issued executor token in that file, outside source control; create new files with owner-only permissions on Unix or a restricted private ACL on Windows. Use a secure input mechanism rather than putting the secret in command arguments, chat, or shell history. Never print it during checks.

Choose a persistent, private configuration directory and journal path. Preserve any existing journal: it contains execution evidence and is bound to the executor identity on the broker. Do not delete, replace, or point the same executor at a fresh journal to resolve a connection error.

Create a new `client.json`, or parse, back up, and merge an existing one without removing unrelated workspaces or settings. Use this shape, substituting every angle-bracket placeholder:

```json
{
  "server_url": "<CLIENT_WS_URL>",
  "executor_id": "<EXECUTOR_ID>",
  "journal_path": "<ABSOLUTE_PERSISTENT_JOURNAL_PATH>",
  "token_file": "<ABSOLUTE_PRIVATE_EXECUTOR_TOKEN_FILE>",
  "workspaces": {
    "<WORKSPACE_ID>": {
      "root": "<ABSOLUTE_EXISTING_WORKSPACE_DIRECTORY>",
      "bash_path": "<ABSOLUTE_BASH_EXECUTABLE>",
      "env": {}
    }
  },
  "max_output_bytes": 10485760,
  "max_journal_bytes": 1073741824
}
```

Use a JSON serializer so Windows backslashes are escaped correctly. The workspace key is the registered logical ID; `root` is its mapping on this B machine. Command execution runs with this machine's account privileges. The workspace path restriction is not an operating-system sandbox.

If the host reliably inherits a private `BRIDGE_EXECUTOR_TOKEN` environment variable, replace `token_file` with `"token_env": "BRIDGE_EXECUTOR_TOKEN"`. Configure exactly one of these two fields. Unlike Claude MCP configuration, `client.json` does not expand `${VARIABLE}` expressions; `token_env` names the variable to read. Relative journal, token-file, and workspace paths are resolved against the directory containing `client.json`, not the host's working directory. Absolute paths avoid ambiguity.

Bash is non-interactive and starts without shell profile files. There is no PTY, interactive stdin, or persistent shell between commands. Add only necessary workspace environment settings, and do not copy an entire login environment or its credentials into `env`.

## Validate local prerequisites and network reachability

Run the actual executable with the absolute configuration path:

```sh
"/absolute/path/task-bridge-client" doctor --config "/absolute/path/client.json"
"/absolute/path/task-bridge-client" doctor --config "/absolute/path/client.json" --connect
```

In PowerShell, use the call operator for a quoted executable path:

```powershell
& 'C:\Private\TaskBridge\task-bridge-client.exe' doctor --config 'C:\Private\TaskBridge\client.json'
& 'C:\Private\TaskBridge\task-bridge-client.exe' doctor --config 'C:\Private\TaskBridge\client.json' --connect
```

`doctor` checks configuration, the workspace and Bash paths, and whether a credential is present; it does not execute a Bash command. `--connect` adds TCP/TLS reachability only. It does not authenticate the token, complete the control WebSocket handshake, register an executor, or verify A. A successful doctor result is not an end-to-end connection test.

Public `GET <BROKER_ORIGIN>/healthz` and `/readyz` can help diagnose availability without a token. They also do not prove that the executor credentials or workspace scope are valid.

## Register the stdio server with the actual host

Inspect the host's supported local MCP configuration format and existing entries. Use its native registration mechanism when available. Do not assume all hosts use the same settings path or JSON shape; for example, a host using TOML must receive its native equivalent rather than this JSON pasted into a TOML file.

For hosts that use an `mcpServers` object, merge this entry while preserving existing settings:

```json
{
  "mcpServers": {
    "bridge-tasks": {
      "command": "<ABSOLUTE_TASK_BRIDGE_CLIENT_EXECUTABLE>",
      "args": [
        "mcp",
        "--config",
        "<ABSOLUTE_CLIENT_JSON_PATH>"
      ]
    }
  }
}
```

`command` is the executable path, not a shell command string. Keep arguments as separate values. Use `.exe` on Windows. If the name is already used by another service, choose an unused name. Back up an existing settings file before a focused merge, and verify it still parses.

Let the host start and own the subprocess; do not start an extra `task-bridge-client mcp` as a background daemon. Stdout is reserved for MCP and diagnostics go to stderr. One executor/journal can be used by only one B process at a time. Reuse or cleanly stop an existing host-owned instance before replacing it; inspect active work before stopping it, because host shutdown also stops its local command execution.

Reload or restart only the relevant host connection using the host's supported workflow. Confirm discovery of these nine tools: `tasks_submit`, `tasks_run`, `tasks_get`, `tasks_wait`, `tasks_resume`, `tasks_cancel`, `tasks_list`, `tasks_events`, and `agents_usage`.

Use `agents_usage` with a configured `workspace_id` and optional `agent_id` to query the latest account usage reported by connected A controllers, without submitting a task. A refreshes with Claude's local `/usage` command after connecting and every minute, including while idle; the initial refresh may take a few seconds. The result includes available five-hour and weekly usage percentages, reset times, source, observation timestamps, and stale flags. An empty agent list means no matching A is connected. An agent with `available: false` has no account window to report; missing or stale fields are unknown. The B query reads a cache and does not call a model. Claude task subprocesses do not participate in these refreshes.

Call `tasks_list` with the configured `workspace_id` and a small `limit`, such as `1`. A successful response, including an empty list, verifies a real authenticated broker RPC from this client. A local tool list alone does not prove the broker connection is usable. If using an external tool timeout, allow the bounded waits to finish; `tasks_wait` can wait up to 60 seconds and `tasks_run` also performs submission.

## Run a minimal end-to-end smoke task

Keep the host-owned B subprocess running and arrange for the matching `task-bridge-agent` controller to be online. It receives the offer and starts a fresh Claude Code process for the task; B does not start that process. Call `tasks_submit` once with real values:

Write every new task prompt in English. Both `tasks_submit` and `tasks_run` require 1–8192 bytes, printable ASCII plus TAB/LF/CR, and at least one `A–Z` or `a–z` letter. Use an ASCII nonce below. Invalid prompts return `invalid_arguments` without creating or dispatching a task. Exact retries of existing tasks retain the original content, including legacy non-English prompts; do not translate a prompt whose submission outcome is uncertain.

```json
{
  "workspace_id": "<WORKSPACE_ID>",
  "idempotency_key": "setup-smoke-<NEW_UUID>",
  "deadline_ms": 300000,
  "prompt": "Use the bridge bash tool on B to run: printf 'task-bridge-smoke-<NONCE>\\n'; hostname; pwd. Do not write files. Wait for the command's known terminal result, read its output, and complete the task with the marker, hostname, and working directory in the summary."
}
```

Save the returned `task_id`. If submission times out or disconnects before its result is known, retry the identical request with the same idempotency key. Do not create a new key merely to escape an uncertain result.

When requested, include optional `model` and `effort` fields in `tasks_submit` or `tasks_run`, for example `"model": "opus", "effort": "high"`. The model may be an alias or full model ID; effort accepts `low`, `medium`, `high`, `xhigh`, or `max`. The task model overrides A's agent default. Omit either field to keep its existing default behavior. These choices are saved with the task and reused on recovery; changing or removing them on a retry with the same key causes `idempotency_conflict`. Availability depends on A's Claude version, account and provider. This applies to the per-task controller; a manual interactive-channel session requires its operator to select the model and effort.

Call `tasks_wait` with that `task_id` and `wait_ms: 10000`; if it remains pending, use the same ID with `tasks_get` or another bounded wait. A successful smoke test has a durable `succeeded` task, a known successful command result, and output identifying this B machine, the configured workspace, and the chosen nonce. Confirm with A's operator that its controller launched the task's Claude process. A wait timeout, queued task, Claude terminal reply, or process exit alone is not success.

For session-isolation acceptance, submit a second task with a different key and nonce. Have A's operator verify a different Claude session ID and working directory, and confirm the second durable result from B. If A intentionally uses the optional manual interactive-channel mode, verify its unsolicited channel delivery separately; that mode keeps one conversation across tasks.

If A is unavailable, stop at “B authenticated; end-to-end test pending” and retain the task ID if one was already submitted. Do not configure or acquire A's token as part of B setup unless the user also requested that separate work.

## Resume an existing task

Retain the original `task_id`. Review its task and command records and B's journal before requesting recovery. Use the same B credential that submitted it, with the original workspace configured and the journal reconciled. A keeps its normal `task-bridge-agent run` process online with the original state directory and Claude profile; no manual A resume command is needed.

Call the B MCP tool `tasks_resume`:

```json
{
  "task_id": "<EXISTING_TASK_ID>",
  "idempotency_key": "resume-<NEW_UUID>"
}
```

The result contains the same `task_id` and a durable `resume_request` with a `request_id` and `state`. `pending` means it is waiting for the original A; it remains pending if A is offline. `accepted` means A has claimed this one attempt, not that Claude has started or the task has finished. Use `tasks_get` with the same task ID to inspect its latest `resume_request`, or continue bounded waiting with `tasks_wait`:

```json
{
  "task_id": "<EXISTING_TASK_ID>",
  "wait_ms": 10000
}
```

If the request's response is lost, retry `tasks_resume` with the same key. That key returns its original receipt even after a newer attempt exists. If the receipt is `failed`, inspect its `error` and resolve the cause before using a new key to explicitly request another attempt. The same request is never consumed twice, and reconnecting A does not retry a failed attempt. A new key is refused while an existing request is pending or accepted.

Recovery keeps the original task, prompt, deadline, Claude session UUID, and working directory. It refreshes assignment and relay credentials. Unknown or running commands, incomplete reconciliation, quarantine, active assignments, expired deadlines, and terminal tasks can block a new request. Missing A state or transcript fails recovery without creating a replacement conversation; see [the A guide](claude.md#resume-one-saved-task). Do not submit a new task or change a command's operation key to bypass uncertain execution.

## Preserve recovery state and report

- `unauthorized` or forbidden scope: check the role, executor/workspace mapping, and token source; ask the operator about revocation. Do not try another role's token.
- Duplicate connection or journal lock: locate the existing host-owned B process; do not create a new journal or kill unrelated processes.
- `journal_mismatch`, server-epoch changes, or unknown execution: retain the journal and logs, stop automatic setup retries, and involve the operator. `reconcile` is an explicit recovery operation with reviewed evidence, not an installation shortcut.
- Task stays queued: check the matching A controller and that B reports this workspace. Do not resubmit under a new key to force scheduling.

Report the binary version and source revision when built locally, changed configuration paths, host entry name, nonsecret endpoint and IDs, doctor results with their limited scope, authenticated `tasks_list` outcome, and any smoke task ID/result. Never include token contents. State clearly whether configuration, authenticated connection, and end-to-end execution have each been verified.

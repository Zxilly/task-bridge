-- +goose Up
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE principals (id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, payload TEXT NOT NULL);
CREATE TABLE workspaces (id TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE tasks (id TEXT PRIMARY KEY, requester TEXT NOT NULL, idem TEXT NOT NULL, request_hash TEXT NOT NULL, workspace TEXT NOT NULL, state TEXT NOT NULL, created INTEGER NOT NULL, payload TEXT NOT NULL, UNIQUE(requester,idem));
CREATE INDEX tasks_schedule ON tasks(state,created);
CREATE UNIQUE INDEX tasks_one_active_workspace ON tasks(workspace) WHERE state IN ('offered','running','interrupted','cancel_requested');
CREATE TABLE commands (id TEXT PRIMARY KEY, task TEXT NOT NULL REFERENCES tasks(id), operation_key TEXT NOT NULL, request_hash TEXT NOT NULL, state TEXT NOT NULL, payload TEXT NOT NULL, UNIQUE(task,operation_key));
CREATE INDEX commands_task ON commands(task);
CREATE TABLE command_chunks (command TEXT NOT NULL REFERENCES commands(id), seq INTEGER NOT NULL, stream TEXT NOT NULL, data BLOB NOT NULL, PRIMARY KEY(command,seq));
CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT NOT NULL REFERENCES tasks(id), type TEXT NOT NULL, payload TEXT NOT NULL, created INTEGER NOT NULL);
CREATE INDEX events_task ON events(task,seq);
CREATE TABLE outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, peer TEXT NOT NULL, message_id TEXT NOT NULL UNIQUE, type TEXT NOT NULL, payload TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0);
CREATE INDEX outbox_pending ON outbox(peer,acked,seq);
CREATE TABLE inbox (peer TEXT NOT NULL, message_id TEXT NOT NULL, digest TEXT NOT NULL, PRIMARY KEY(peer,message_id));
CREATE TABLE assignments (id TEXT PRIMARY KEY, task TEXT NOT NULL REFERENCES tasks(id), agent TEXT NOT NULL, connection TEXT NOT NULL, epoch INTEGER NOT NULL, lease_until INTEGER NOT NULL);
CREATE TABLE resolutions (id INTEGER PRIMARY KEY AUTOINCREMENT, target TEXT NOT NULL, outcome TEXT NOT NULL, reason TEXT NOT NULL, actor TEXT NOT NULL, created INTEGER NOT NULL);
CREATE TABLE peer_cursors (peer TEXT PRIMARY KEY, stream TEXT NOT NULL, last_ack INTEGER NOT NULL DEFAULT 0);

-- +goose Down
DROP TABLE peer_cursors;
DROP TABLE resolutions;
DROP TABLE assignments;
DROP TABLE inbox;
DROP TABLE outbox;
DROP TABLE events;
DROP TABLE command_chunks;
DROP TABLE commands;
DROP TABLE tasks;
DROP TABLE workspaces;
DROP TABLE principals;
DROP TABLE metadata;

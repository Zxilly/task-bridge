-- +goose Up
ALTER TABLE command_chunks ADD COLUMN digest TEXT NOT NULL DEFAULT '';
-- A snapshot can contain several queued tasks that all need reconciliation.
-- Only granted/live tasks occupy the unique execution slot; paused tasks also
-- block the scheduler through workspace quarantine and its busy-workspace set.
DROP INDEX tasks_one_active_workspace;
CREATE UNIQUE INDEX tasks_one_active_workspace ON tasks(workspace) WHERE state IN ('offered','running','cancel_requested');

-- +goose Down
DROP INDEX tasks_one_active_workspace;
CREATE UNIQUE INDEX tasks_one_active_workspace ON tasks(workspace) WHERE state IN ('offered','running','cancel_requested');

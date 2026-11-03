-- +goose Up
DROP INDEX tasks_one_active_workspace;

-- +goose Down
-- Downgrade requires draining concurrent tasks first.
CREATE UNIQUE INDEX tasks_one_active_workspace ON tasks(workspace) WHERE state IN ('offered','running','cancel_requested');

-- +goose Up
CREATE TABLE task_resume_requests (
    id TEXT PRIMARY KEY,
    task TEXT NOT NULL REFERENCES tasks(id),
    requester TEXT NOT NULL,
    idem TEXT NOT NULL,
    payload BLOB NOT NULL,
    UNIQUE(requester, idem)
);
CREATE INDEX task_resume_requests_task ON task_resume_requests(task);

-- +goose Down
DROP TABLE task_resume_requests;

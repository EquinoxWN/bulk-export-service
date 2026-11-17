-- Export jobs live on the primary. Rows being exported are read from the replica, never from here.
CREATE TABLE IF NOT EXISTS export_jobs (
    seq          bigint GENERATED ALWAYS AS IDENTITY UNIQUE,  -- claim order, not a wall-clock time
    id           text PRIMARY KEY CHECK (id ~ '^[0-9a-f]{32}$'),
    account_id   bigint NOT NULL CHECK (account_id > 0),
    dataset      text NOT NULL,
    format       text NOT NULL,
    idem_key     text NOT NULL,
    status       text NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
    worker       text NOT NULL DEFAULT '',
    row_count    bigint NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    page_count   bigint NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    cursor       jsonb,
    bytes        bigint NOT NULL DEFAULT 0,
    sha256       text NOT NULL DEFAULT '',
    file         text NOT NULL DEFAULT '',
    error        text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    started_at   timestamptz,
    finished_at  timestamptz,
    UNIQUE (account_id, idem_key)
);
CREATE INDEX IF NOT EXISTS export_jobs_queued ON export_jobs (seq) WHERE status = 'queued';

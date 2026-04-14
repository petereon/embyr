CREATE TABLE IF NOT EXISTS documents (
    path        TEXT        PRIMARY KEY,
    collection  TEXT        NOT NULL,
    parent      TEXT,
    data        JSONB,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    version     BIGINT      NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_documents_collection ON documents(collection);
CREATE INDEX IF NOT EXISTS idx_documents_parent     ON documents(parent);
CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updated_at);
CREATE INDEX IF NOT EXISTS idx_documents_data ON documents USING GIN(data);

CREATE TABLE IF NOT EXISTS indexes (
    id          TEXT    PRIMARY KEY,
    collection  TEXT    NOT NULL,
    fields      JSONB   NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'READY'
);

CREATE TABLE IF NOT EXISTS transactions (
    id          TEXT        PRIMARY KEY,
    started_at  TIMESTAMPTZ NOT NULL,
    reads       JSONB,
    expires_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS documents (
    path        TEXT    PRIMARY KEY,
    collection  TEXT    NOT NULL,
    parent      TEXT,
    data        TEXT,
    created_at  TEXT    NOT NULL,
    updated_at  TEXT    NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_documents_collection ON documents(collection);
CREATE INDEX IF NOT EXISTS idx_documents_parent     ON documents(parent);
CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updated_at);

CREATE TABLE IF NOT EXISTS indexes (
    id          TEXT    PRIMARY KEY,
    collection  TEXT    NOT NULL,
    fields      TEXT    NOT NULL,
    state       TEXT    NOT NULL DEFAULT 'READY'
);

CREATE TABLE IF NOT EXISTS transactions (
    id          TEXT    PRIMARY KEY,
    started_at  TEXT    NOT NULL,
    reads       TEXT,
    expires_at  TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_expires_at ON transactions(expires_at);

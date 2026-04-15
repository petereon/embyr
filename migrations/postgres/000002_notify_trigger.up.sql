CREATE OR REPLACE FUNCTION notify_doc_change() RETURNS TRIGGER AS $$
DECLARE
    payload TEXT;
    kind TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        kind := 'delete';
        payload := json_build_object(
            'path',       OLD.path,
            'collection', OLD.collection,
            'parent',     OLD.parent,
            'kind',       kind,
            'version',    OLD.version,
            'data',       ''
        )::text;
    ELSE
        kind := 'upsert';
        payload := json_build_object(
            'path',       NEW.path,
            'collection', NEW.collection,
            'parent',     NEW.parent,
            'kind',       kind,
            'version',    NEW.version,
            'data',       NEW.data::text
        )::text;
    END IF;
    -- pg_notify payload is limited to 8000 bytes.
    -- Truncate data to stay under the limit; the Listen handler
    -- re-fetches the full document when data is empty.
    IF length(payload) > 7500 THEN
        IF TG_OP = 'DELETE' THEN
            payload := json_build_object(
                'path', OLD.path, 'collection', OLD.collection,
                'parent', OLD.parent, 'kind', kind, 'version', OLD.version, 'data', ''
            )::text;
        ELSE
            payload := json_build_object(
                'path', NEW.path, 'collection', NEW.collection,
                'parent', NEW.parent, 'kind', kind, 'version', NEW.version, 'data', ''
            )::text;
        END IF;
    END IF;
    PERFORM pg_notify('doc_changes', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_doc_changes
AFTER INSERT OR UPDATE OR DELETE ON documents
FOR EACH ROW EXECUTE FUNCTION notify_doc_change();

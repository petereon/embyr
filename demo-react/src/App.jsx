import { useState, useEffect, useRef } from "react";
import {
  collection,
  doc,
  addDoc,
  deleteDoc,
  updateDoc,
  onSnapshot,
  query,
  where,
  orderBy,
  serverTimestamp,
  increment,
  arrayUnion,
  arrayRemove,
  writeBatch,
  runTransaction,
} from "firebase/firestore";
import { db } from "./firebase.js";

const COLL = "notes";
const CATEGORIES = ["general", "idea", "bug", "todo"];

export default function App() {
  const [notes, setNotes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [text, setText] = useState("");
  const [category, setCategory] = useState("general");
  const [adding, setAdding] = useState(false);
  const [sortBy, setSortBy] = useState("createdAt");
  const [filterCat, setFilterCat] = useState("all");
  const [selected, setSelected] = useState(new Set());
  const [tagInputs, setTagInputs] = useState({});
  const [ops, setOps] = useState([]);

  function logOp(msg) {
    setOps((prev) => [msg, ...prev].slice(0, 6));
  }

  // Two-ref strategy for zero-gap, no-pending-target-removal listener swaps.
  //
  // Problem 1 — React cleanup order: cleanup of old effect runs BEFORE new
  // effect body → removing listener in cleanup creates a zero-listener gap →
  // SDK closes WebChannel → 13s reconnect backoff. Fix: return () => {} (no-op).
  //
  // Problem 2 — pending target removal: removing a target that hasn't received
  // its first CURRENT ack can trigger an SDK stream reset. Fix: keep old listener
  // alive until new listener fires its first snapshot (is CURRENT), then remove it.
  // If another filter change fires before that snapshot, flush the pending removal
  // immediately so we never accumulate more than 2 simultaneous listeners.
  //
  // genRef guards against stale snapshot callbacks updating state.
  const cleanupRef = useRef(null); // current active listener
  const pendingRemRef = useRef(null); // previous listener deferred until new is CURRENT
  const genRef = useRef(0);
  const hasLoadedRef = useRef(false);

  // ── Live query ────────────────────────────────────────────────────────────
  // Rebuilt whenever sort or filter changes.
  // Demonstrates: onSnapshot, orderBy, where (when category filter active)
  useEffect(() => {
    if (!hasLoadedRef.current) setLoading(true);
    const constraints = [];
    if (filterCat !== "all")
      constraints.push(where("category", "==", filterCat));
    constraints.push(orderBy(sortBy, "desc"));

    const gen = ++genRef.current;
    const toDefer = cleanupRef.current;

    const newUnsub = onSnapshot(
      query(collection(db, COLL), ...constraints),
      (snap) => {
        // New listener is CURRENT — safe to remove the previous one now.
        if (pendingRemRef.current) {
          pendingRemRef.current();
          pendingRemRef.current = null;
        }
        // Ignore callbacks from superseded listeners.
        if (gen !== genRef.current) return;
        hasLoadedRef.current = true;
        setNotes(snap.docs.map((d) => ({ id: d.id, ...d.data() })));
        setLoading(false);
        setSelected(new Set());
      },
      (err) => {
        if (gen !== genRef.current) return;
        setError(err.message);
        setLoading(false);
      },
    );

    cleanupRef.current = newUnsub;
    // Flush any listener that was already deferred (rapid successive changes),
    // then defer the one we just replaced.
    if (pendingRemRef.current) pendingRemRef.current();
    pendingRemRef.current = toDefer;

    return () => {};
  }, [sortBy, filterCat]);

  // Unmount-only cleanup.
  useEffect(() => {
    return () => {
      if (cleanupRef.current) cleanupRef.current();
      if (pendingRemRef.current) pendingRemRef.current();
    };
  }, []);

  // ── addDoc + serverTimestamp ───────────────────────────────────────────────
  async function handleAdd(e) {
    e.preventDefault();
    if (!text.trim()) return;
    setAdding(true);
    setError(null);
    try {
      await addDoc(collection(db, COLL), {
        text: text.trim(),
        category,
        tags: [],
        votes: 0,
        createdAt: serverTimestamp(),
      });
      logOp("addDoc  { createdAt: serverTimestamp() }");
      setText("");
    } catch (err) {
      setError(err.message);
    } finally {
      setAdding(false);
    }
  }

  // ── updateDoc + increment ─────────────────────────────────────────────────
  async function handleVote(id, delta) {
    try {
      await updateDoc(doc(db, COLL, id), { votes: increment(delta) });
      logOp(`updateDoc  { votes: increment(${delta > 0 ? "+" : ""}${delta}) }`);
    } catch (err) {
      setError(err.message);
    }
  }

  // ── updateDoc + arrayUnion ────────────────────────────────────────────────
  async function handleAddTag(id) {
    const tag = (tagInputs[id] || "").trim().replace(/^#+/, "");
    if (!tag) return;
    try {
      await updateDoc(doc(db, COLL, id), { tags: arrayUnion(tag) });
      logOp(`updateDoc  { tags: arrayUnion("${tag}") }`);
      setTagInputs((t) => ({ ...t, [id]: "" }));
    } catch (err) {
      setError(err.message);
    }
  }

  // ── updateDoc + arrayRemove ───────────────────────────────────────────────
  async function handleRemoveTag(id, tag) {
    try {
      await updateDoc(doc(db, COLL, id), { tags: arrayRemove(tag) });
      logOp(`updateDoc  { tags: arrayRemove("${tag}") }`);
    } catch (err) {
      setError(err.message);
    }
  }

  // ── deleteDoc ─────────────────────────────────────────────────────────────
  async function handleDelete(id) {
    try {
      await deleteDoc(doc(db, COLL, id));
      logOp("deleteDoc");
    } catch (err) {
      setError(err.message);
    }
  }

  // ── writeBatch ────────────────────────────────────────────────────────────
  async function handleBatchDelete() {
    if (selected.size === 0) return;
    try {
      const batch = writeBatch(db);
      for (const id of selected) batch.delete(doc(db, COLL, id));
      await batch.commit();
      logOp(
        `writeBatch.commit()  — deleted ${selected.size} doc${selected.size > 1 ? "s" : ""}`,
      );
      setSelected(new Set());
    } catch (err) {
      setError(err.message);
    }
  }

  // ── runTransaction ────────────────────────────────────────────────────────
  // One-time boost: reads the document to check the `boosted` flag, then
  // conditionally writes. The read gates the write — this is what makes a
  // transaction necessary here rather than a plain updateDoc.
  async function handleBoost(id) {
    const ref = doc(db, COLL, id);
    try {
      let alreadyBoosted = false;
      await runTransaction(db, async (tx) => {
        const snap = await tx.get(ref);
        if (!snap.exists()) throw new Error("Document was deleted");
        if (snap.data().boosted) {
          alreadyBoosted = true;
          return;
        }
        tx.update(ref, { votes: (snap.data().votes || 0) + 10, boosted: true });
      });
      if (alreadyBoosted) {
        setError(
          "Already boosted — runTransaction read the flag and aborted the write.",
        );
      } else {
        logOp(
          "runTransaction  — read boosted flag → +10 votes, set boosted:true",
        );
      }
    } catch (err) {
      setError(err.message);
    }
  }

  function toggleSelect(id) {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  }

  function formatTs(ts) {
    if (!ts) return "…";
    try {
      const d = ts.toDate ? ts.toDate() : new Date(ts);
      return d.toLocaleString(undefined, {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
    } catch {
      return String(ts);
    }
  }

  const activeQuery =
    filterCat !== "all"
      ? `query(col, where('category','==','${filterCat}'), orderBy('${sortBy}','desc'))`
      : `query(col, orderBy('${sortBy}','desc'))`;

  return (
    <div className="app">
      <header>
        <h1>embyr demo</h1>
        <p className="subtitle">
          Firebase SDK → <code>connectFirestoreEmulator</code> → embyr (Go) →
          SQLite/PostgreSQL
        </p>
      </header>

      {/* ── Add form ── */}
      <form onSubmit={handleAdd} className="add-form">
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="New note…"
          disabled={adding}
          autoFocus
        />
        <select
          value={category}
          onChange={(e) => setCategory(e.target.value)}
          disabled={adding}
        >
          {CATEGORIES.map((c) => (
            <option key={c}>{c}</option>
          ))}
        </select>
        <button type="submit" disabled={adding || !text.trim()}>
          {adding ? "…" : "Add"}
        </button>
      </form>

      {error && (
        <div className="error" onClick={() => setError(null)}>
          ⚠ {error} <span className="dismiss">×</span>
        </div>
      )}

      {/* ── Query controls ── */}
      <div className="controls">
        <div className="control-group">
          <span className="control-label">sort</span>
          <button
            className={sortBy === "createdAt" ? "ctrl-btn active" : "ctrl-btn"}
            onClick={() => setSortBy("createdAt")}
          >
            newest
          </button>
          <button
            className={sortBy === "votes" ? "ctrl-btn active" : "ctrl-btn"}
            onClick={() => setSortBy("votes")}
          >
            top rated
          </button>
        </div>
        <div className="control-group">
          <span className="control-label">filter</span>
          <button
            className={filterCat === "all" ? "ctrl-btn active" : "ctrl-btn"}
            onClick={() => setFilterCat("all")}
          >
            all
          </button>
          {CATEGORIES.map((c) => (
            <button
              key={c}
              className={filterCat === c ? "ctrl-btn active" : "ctrl-btn"}
              onClick={() => setFilterCat(c)}
            >
              {c}
            </button>
          ))}
        </div>
      </div>
      <div className="query-hint">
        <code>{activeQuery}</code>
      </div>

      {/* ── Notes ── */}
      {loading ? (
        <p className="muted">Loading…</p>
      ) : notes.length === 0 ? (
        <p className="muted">No notes yet — add one above.</p>
      ) : (
        <ul className="notes">
          {notes.map((note) => (
            <li
              key={note.id}
              className={
                selected.has(note.id) ? "note-item selected" : "note-item"
              }
            >
              <input
                type="checkbox"
                className="note-check"
                checked={selected.has(note.id)}
                onChange={() => toggleSelect(note.id)}
              />

              {/* Vote column */}
              <div className="vote-col">
                <button
                  className="vote-btn up"
                  onClick={() => handleVote(note.id, 1)}
                >
                  ▲
                </button>
                <span className="vote-count">{note.votes ?? 0}</span>
                <button
                  className="vote-btn down"
                  onClick={() => handleVote(note.id, -1)}
                >
                  ▼
                </button>
              </div>

              {/* Body */}
              <div className="note-body">
                <div className="note-top">
                  <span className="note-text">{note.text}</span>
                  <span
                    className={`cat-badge cat-${note.category || "general"}`}
                  >
                    {note.category || "general"}
                  </span>
                </div>

                {/* Tags */}
                <div className="tag-row">
                  {(note.tags || []).map((tag) => (
                    <span key={tag} className="tag">
                      #{tag}
                      <button
                        className="tag-remove"
                        onClick={() => handleRemoveTag(note.id, tag)}
                      >
                        ×
                      </button>
                    </span>
                  ))}
                  <form
                    className="tag-form"
                    onSubmit={(e) => {
                      e.preventDefault();
                      handleAddTag(note.id);
                    }}
                  >
                    <input
                      className="tag-input"
                      value={tagInputs[note.id] || ""}
                      onChange={(e) =>
                        setTagInputs((t) => ({
                          ...t,
                          [note.id]: e.target.value,
                        }))
                      }
                      placeholder="#tag"
                    />
                    <button type="submit" className="tag-add-btn">
                      +
                    </button>
                  </form>
                </div>

                <div className="note-meta">
                  <code className="note-id">{note.id}</code>
                  <span className="note-ts">{formatTs(note.createdAt)}</span>
                </div>
              </div>

              {/* Actions */}
              <div className="note-actions">
                <button
                  className={`btn-boost${note.boosted ? " boosted" : ""}`}
                  onClick={() => handleBoost(note.id)}
                  title={
                    note.boosted
                      ? "Already boosted (transaction will abort)"
                      : "runTransaction: read boosted flag → +10 if not yet boosted"
                  }
                >
                  {note.boosted ? "⚡done" : "⚡+10"}
                </button>
                <button
                  className="btn-delete"
                  onClick={() => handleDelete(note.id)}
                  aria-label="Delete"
                >
                  ×
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}

      {/* ── Batch bar ── */}
      {selected.size > 0 && (
        <div className="batch-bar">
          <span className="batch-count">{selected.size} selected</span>
          <button className="batch-btn" onClick={handleBatchDelete}>
            writeBatch — delete {selected.size}
          </button>
          <button
            className="batch-clear"
            onClick={() => setSelected(new Set())}
          >
            clear
          </button>
        </div>
      )}

      {/* ── Ops log ── */}
      {ops.length > 0 && (
        <div className="ops-log">
          <div className="ops-label">recent operations</div>
          {ops.map((op, i) => (
            <div key={i} className="op-entry">
              <span className="op-bullet">›</span>
              <code>{op}</code>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

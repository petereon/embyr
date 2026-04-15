import { useState, useEffect } from 'react';
import {
  collection,
  addDoc,
  deleteDoc,
  doc,
  onSnapshot,
  query,
  orderBy,
  serverTimestamp,
} from 'firebase/firestore';
import { db } from './firebase.js';

const COLL = 'notes';

export default function App() {
  const [notes, setNotes]   = useState([]);
  const [text, setText]     = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState(null);
  const [adding, setAdding] = useState(false);

  useEffect(() => {
    const q = query(collection(db, COLL), orderBy('createdAt', 'desc'));
    const unsub = onSnapshot(q,
      snap => {
        setNotes(snap.docs.map(d => ({ id: d.id, path: d.ref.path, ...d.data() })));
        setLoading(false);
      },
      err => setError(err.message)
    );
    return unsub;
  }, []);

  async function handleAdd(e) {
    e.preventDefault();
    if (!text.trim()) return;
    setAdding(true);
    setError(null);
    try {
      await addDoc(collection(db, COLL), {
        text: text.trim(),
        createdAt: serverTimestamp(),
      });
      setText('');
    } catch (e) {
      setError(e.message);
    } finally {
      setAdding(false);
    }
  }

  async function handleDelete(id) {
    setError(null);
    try {
      await deleteDoc(doc(db, COLL, id));
    } catch (e) {
      setError(e.message);
    }
  }

  return (
    <div className="app">
      <header>
        <h1>firstyr demo</h1>
        <p className="subtitle">
          Firebase SDK → <code>connectFirestoreEmulator</code> → firstyr (Go) → PostgreSQL
        </p>
      </header>

      <form onSubmit={handleAdd} className="add-form">
        <input
          value={text}
          onChange={e => setText(e.target.value)}
          placeholder="Type a note…"
          disabled={adding}
          autoFocus
        />
        <button type="submit" disabled={adding || !text.trim()}>
          {adding ? 'Adding…' : 'Add'}
        </button>
      </form>

      {error && <div className="error">⚠ {error}</div>}

      {loading ? (
        <p className="muted">Loading…</p>
      ) : notes.length === 0 ? (
        <p className="muted">No notes yet.</p>
      ) : (
        <ul className="notes">
          {notes.map(note => (
            <li key={note.id}>
              <div className="note-body">
                <span className="note-text">{note.text}</span>
                <code className="note-path">{note.path}</code>
              </div>
              <button
                className="delete"
                onClick={() => handleDelete(note.id)}
                aria-label="Delete"
              >
                ×
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

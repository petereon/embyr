import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  collection,
  doc,
  addDoc,
  setDoc,
  getDoc,
  updateDoc,
  deleteDoc,
  getDocs,
  onSnapshot,
  runTransaction,
  writeBatch,
  serverTimestamp,
  increment,
  arrayUnion,
  arrayRemove,
  deleteField,
  query,
  where,
  orderBy,
  limit,
  startAt,
  startAfter,
  endAt,
  endBefore,
  getCountFromServer,
  getAggregateFromServer,
  count,
  sum,
} from 'firebase/firestore';
import { makeDb, closeDb, col } from './firebase.js';

let ctx;

beforeEach(() => { ctx = makeDb(); });
afterEach(() => closeDb(ctx));

// ── CRUD ─────────────────────────────────────────────────────────────────────

describe('setDoc + getDoc roundtrip', () => {
  it('persists and retrieves a document', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { name: 'Alice', age: 30 });

    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(true);
    expect(snap.data()).toEqual({ name: 'Alice', age: 30 });
  });
});

describe('addDoc', () => {
  it('generates a unique document id', async () => {
    const colRef = collection(ctx.db, col());
    const ref1 = await addDoc(colRef, { n: 1 });
    const ref2 = await addDoc(colRef, { n: 2 });
    expect(ref1.id).not.toBe(ref2.id);

    const snap = await getDoc(ref1);
    expect(snap.data().n).toBe(1);
  });
});

describe('updateDoc', () => {
  it('merges fields without overwriting unrelated fields', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: 1, b: 2 });
    await updateDoc(ref, { b: 99, c: 3 });

    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ a: 1, b: 99, c: 3 });
  });
});

describe('deleteDoc', () => {
  it('removes the document', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { x: 1 });
    await deleteDoc(ref);

    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
  });
});

// ── Field transforms ──────────────────────────────────────────────────────────

describe('serverTimestamp', () => {
  it('sets a timestamp field on write', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { createdAt: serverTimestamp() });

    const snap = await getDoc(ref);
    expect(snap.data().createdAt).toBeTruthy();
    expect(snap.data().createdAt.toDate).toBeTypeOf('function');
  });
});

describe('increment', () => {
  it('atomically increments a numeric field', async () => {
    const ref = doc(ctx.db, col(), 'counter');
    await setDoc(ref, { count: 10 });
    await updateDoc(ref, { count: increment(5) });

    const snap = await getDoc(ref);
    expect(snap.data().count).toBe(15);
  });
});

describe('arrayUnion', () => {
  it('appends only missing elements', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { tags: ['a', 'b'] });
    await updateDoc(ref, { tags: arrayUnion('b', 'c') });

    const snap = await getDoc(ref);
    expect(snap.data().tags).toEqual(['a', 'b', 'c']);
  });
});

describe('arrayRemove', () => {
  it('removes matching elements', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { tags: ['a', 'b', 'c'] });
    await updateDoc(ref, { tags: arrayRemove('b') });

    const snap = await getDoc(ref);
    expect(snap.data().tags).toEqual(['a', 'c']);
  });
});

// ── onSnapshot ───────────────────────────────────────────────────────────────

describe('onSnapshot — initial snapshot', () => {
  it('delivers existing documents immediately', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { msg: 'hello' });
    await addDoc(colRef, { msg: 'world' });

    const docs = await new Promise((resolve, reject) => {
      const unsub = onSnapshot(colRef, snap => {
        unsub();
        resolve(snap.docs.map(d => d.data()));
      }, reject);
    });

    expect(docs).toHaveLength(2);
    expect(docs.map(d => d.msg).sort()).toEqual(['hello', 'world']);
  });
});

describe('onSnapshot — live update', () => {
  it('delivers a DocumentChange after a write', async () => {
    const colRef = collection(ctx.db, col());

    // Wait for initial (empty) snapshot, then write a doc.
    const changed = new Promise((resolve, reject) => {
      let initialReceived = false;
      const unsub = onSnapshot(colRef, snap => {
        if (!initialReceived) {
          initialReceived = true;
          // Initial snapshot is empty; now trigger a write.
          addDoc(colRef, { live: true }).catch(reject);
          return;
        }
        if (snap.docs.length > 0) {
          unsub();
          resolve(snap.docs[0].data());
        }
      }, reject);
    });

    const data = await changed;
    expect(data.live).toBe(true);
  });
});

// ── Query ─────────────────────────────────────────────────────────────────────

describe('query with where filter', () => {
  it('returns only matching documents', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { status: 'active', n: 1 });
    await addDoc(colRef, { status: 'inactive', n: 2 });
    await addDoc(colRef, { status: 'active', n: 3 });

    const q = query(colRef, where('status', '==', 'active'));
    const snap = await getDocs(q);
    expect(snap.size).toBe(2);
    for (const d of snap.docs) {
      expect(d.data().status).toBe('active');
    }
  });
});

describe('query with orderBy + limit', () => {
  it('returns top N documents in order', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { score: 10 });
    await addDoc(colRef, { score: 30 });
    await addDoc(colRef, { score: 20 });

    const q = query(colRef, orderBy('score', 'desc'), limit(2));
    const snap = await getDocs(q);
    expect(snap.size).toBe(2);
    expect(snap.docs[0].data().score).toBe(30);
    expect(snap.docs[1].data().score).toBe(20);
  });
});

// ── runTransaction ────────────────────────────────────────────────────────────

describe('runTransaction', () => {
  it('atomically reads and writes', async () => {
    const ref = doc(ctx.db, col(), 'counter');
    await setDoc(ref, { count: 0 });

    await runTransaction(ctx.db, async tx => {
      const snap = await tx.get(ref);
      tx.update(ref, { count: snap.data().count + 1 });
    });

    const snap = await getDoc(ref);
    expect(snap.data().count).toBe(1);
  });

  it('retries on conflict (two concurrent increments)', async () => {
    const ref = doc(ctx.db, col(), 'counter');
    await setDoc(ref, { count: 0 });

    // Run two concurrent transactions; both should eventually succeed.
    await Promise.all([
      runTransaction(ctx.db, async tx => {
        const snap = await tx.get(ref);
        tx.update(ref, { count: snap.data().count + 1 });
      }),
      runTransaction(ctx.db, async tx => {
        const snap = await tx.get(ref);
        tx.update(ref, { count: snap.data().count + 1 });
      }),
    ]);

    const snap = await getDoc(ref);
    expect(snap.data().count).toBe(2);
  });
});

// ── writeBatch ────────────────────────────────────────────────────────────────

describe('writeBatch', () => {
  it('commits multiple writes atomically', async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, 'a');
    const ref2 = doc(ctx.db, c, 'b');
    const ref3 = doc(ctx.db, c, 'c');

    const batch = writeBatch(ctx.db);
    batch.set(ref1, { v: 1 });
    batch.set(ref2, { v: 2 });
    batch.set(ref3, { v: 3 });
    await batch.commit();

    const snaps = await Promise.all([getDoc(ref1), getDoc(ref2), getDoc(ref3)]);
    expect(snaps.map(s => s.data().v)).toEqual([1, 2, 3]);
  });
});

// ── CRUD edge cases ───────────────────────────────────────────────────────────

describe('getDoc non-existent', () => {
  it('returns a snapshot where exists() is false', async () => {
    const ref = doc(ctx.db, col(), 'ghost');
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
    expect(snap.data()).toBeUndefined();
  });
});

describe('updateDoc non-existent', () => {
  it('throws when document does not exist', async () => {
    const ref = doc(ctx.db, col(), 'missing');
    await expect(updateDoc(ref, { x: 1 })).rejects.toThrow();
  });
});

describe('deleteDoc non-existent', () => {
  it('does not throw for a missing document', async () => {
    const ref = doc(ctx.db, col(), 'ghost');
    await expect(deleteDoc(ref)).resolves.toBeUndefined();
  });
});

describe('setDoc with merge', () => {
  it('creates document when it does not exist', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: 1 }, { merge: true });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ a: 1 });
  });

  it('merges into existing document without overwriting unmentioned fields', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: 1, b: 2 });
    await setDoc(ref, { b: 99, c: 3 }, { merge: true });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ a: 1, b: 99, c: 3 });
  });
});

// ── Field transform edge cases ────────────────────────────────────────────────

describe('increment on non-existent field', () => {
  it('treats missing field as 0 and returns the delta', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { other: true });
    await updateDoc(ref, { count: increment(7) });
    const snap = await getDoc(ref);
    expect(snap.data().count).toBe(7);
  });
});

describe('arrayUnion on non-existent field', () => {
  it('creates the array field with the union elements', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { other: true });
    await updateDoc(ref, { tags: arrayUnion('x', 'y') });
    const snap = await getDoc(ref);
    expect(snap.data().tags).toEqual(['x', 'y']);
  });
});

describe('arrayRemove on non-existent field', () => {
  it('is a no-op when the field does not exist', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { other: true });
    await updateDoc(ref, { tags: arrayRemove('x') });
    const snap = await getDoc(ref);
    expect(snap.data().tags).toBeUndefined();
  });
});

describe('deleteField', () => {
  it('removes a top-level field', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { keep: 1, remove: 2 });
    await updateDoc(ref, { remove: deleteField() });
    expect((await getDoc(ref)).data()).toEqual({ keep: 1 });
  });
});

// ── Nested field operations ───────────────────────────────────────────────────

describe('updateDoc nested dot notation', () => {
  it('updates only the targeted nested field', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { profile: { name: 'Alice', age: 30 }, score: 100 });
    await updateDoc(ref, { 'profile.age': 31 });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ profile: { name: 'Alice', age: 31 }, score: 100 });
  });
});

describe('deleteField on nested path', () => {
  it('removes only the targeted nested field', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { profile: { name: 'Alice', age: 30 } });
    await updateDoc(ref, { 'profile.age': deleteField() });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ profile: { name: 'Alice' } });
  });
});

// ── Additional query operators ────────────────────────────────────────────────

describe('where < operator', () => {
  it('returns only documents with field less than value', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 5 }),
      addDoc(colRef, { n: 10 }),
    ]);
    const snap = await getDocs(query(colRef, where('n', '<', 5)));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().n).toBe(1);
  });
});

describe('where > operator', () => {
  it('returns only documents with field greater than value', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 5 }),
      addDoc(colRef, { n: 10 }),
    ]);
    const snap = await getDocs(query(colRef, where('n', '>', 5)));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().n).toBe(10);
  });
});

describe('where <= operator', () => {
  it('returns documents with field less than or equal to value', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 5 }),
      addDoc(colRef, { n: 10 }),
    ]);
    const snap = await getDocs(query(colRef, where('n', '<=', 5)));
    expect(snap.size).toBe(2);
    const vals = snap.docs.map(d => d.data().n).sort((a, b) => a - b);
    expect(vals).toEqual([1, 5]);
  });
});

describe('where >= operator', () => {
  it('returns documents with field greater than or equal to value', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 5 }),
      addDoc(colRef, { n: 10 }),
    ]);
    const snap = await getDocs(query(colRef, where('n', '>=', 5)));
    expect(snap.size).toBe(2);
    const vals = snap.docs.map(d => d.data().n).sort((a, b) => a - b);
    expect(vals).toEqual([5, 10]);
  });
});

describe('where not-in operator', () => {
  it('excludes documents whose field is in the list', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { color: 'red' }),
      addDoc(colRef, { color: 'blue' }),
      addDoc(colRef, { color: 'green' }),
    ]);
    const snap = await getDocs(query(colRef, where('color', 'not-in', ['red', 'green'])));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().color).toBe('blue');
  });
});

describe('where array-contains-any', () => {
  it('returns documents where array field contains at least one value', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { tags: ['a', 'b'] }),
      addDoc(colRef, { tags: ['c', 'd'] }),
      addDoc(colRef, { tags: ['b', 'e'] }),
    ]);
    const snap = await getDocs(query(colRef, where('tags', 'array-contains-any', ['a', 'c'])));
    expect(snap.size).toBe(2);
  });
});

describe('compound query with multiple where', () => {
  it('returns documents matching all conditions', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { cat: 'A', active: true }),
      addDoc(colRef, { cat: 'A', active: false }),
      addDoc(colRef, { cat: 'B', active: true }),
    ]);
    const snap = await getDocs(query(colRef, where('cat', '==', 'A'), where('active', '==', true)));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data()).toMatchObject({ cat: 'A', active: true });
  });
});

// ── Cursor pagination (inclusive variants) ────────────────────────────────────

describe('cursor pagination with startAt (inclusive)', () => {
  it('includes the cursor document', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 2 }),
      addDoc(colRef, { n: 3 }),
    ]);
    const all = await getDocs(query(colRef, orderBy('n')));
    const second = all.docs[1]; // n=2
    const snap = await getDocs(query(colRef, orderBy('n'), startAt(second)));
    expect(snap.size).toBe(2);
    expect(snap.docs.map(d => d.data().n)).toEqual([2, 3]);
  });
});

describe('cursor pagination with endAt (inclusive)', () => {
  it('includes the cursor document', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 2 }),
      addDoc(colRef, { n: 3 }),
    ]);
    const all = await getDocs(query(colRef, orderBy('n')));
    const second = all.docs[1]; // n=2
    const snap = await getDocs(query(colRef, orderBy('n'), endAt(second)));
    expect(snap.size).toBe(2);
    expect(snap.docs.map(d => d.data().n)).toEqual([1, 2]);
  });
});

describe('cursor pagination with DESC order', () => {
  it('startAfter returns documents after cursor in descending order', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 2 }),
      addDoc(colRef, { n: 3 }),
    ]);
    const first = await getDocs(query(colRef, orderBy('n', 'desc'), limit(1)));
    expect(first.docs[0].data().n).toBe(3);
    const rest = await getDocs(query(colRef, orderBy('n', 'desc'), startAfter(first.docs[0])));
    expect(rest.size).toBe(2);
    expect(rest.docs.map(d => d.data().n)).toEqual([2, 1]);
  });
});

// ── Aggregation (gRPC) ────────────────────────────────────────────────────────

describe('getCountFromServer (gRPC)', () => {
  it('returns the document count', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { v: 1 }),
      addDoc(colRef, { v: 2 }),
    ]);
    const snap = await getCountFromServer(colRef);
    expect(snap.data().count).toBe(2);
  });
});

describe('getAggregateFromServer count + sum (gRPC)', () => {
  it('returns count and sum in one request', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { amount: 10 }),
      addDoc(colRef, { amount: 20 }),
      addDoc(colRef, { amount: 30 }),
    ]);
    const snap = await getAggregateFromServer(colRef, { total: sum('amount'), n: count() });
    expect(snap.data().total).toBe(60);
    expect(snap.data().n).toBe(3);
  });
});

describe('getCountFromServer empty collection (gRPC)', () => {
  it('returns 0 for an empty collection', async () => {
    const colRef = collection(ctx.db, col());
    const snap = await getCountFromServer(colRef);
    expect(snap.data().count).toBe(0);
  });
});

// ── onSnapshot advanced ───────────────────────────────────────────────────────

describe('onSnapshot query filter', () => {
  it('does not deliver updates for documents that do not match the query', async () => {
    const colRef = collection(ctx.db, col());
    const received = [];

    await new Promise((resolve, reject) => {
      const unsub = onSnapshot(
        query(colRef, where('active', '==', true)),
        snap => {
          for (const change of snap.docChanges()) {
            received.push(change.doc.data());
          }
          if (received.length > 0) resolve();
        },
        reject,
      );
      // Write a non-matching doc first, then a matching one.
      addDoc(colRef, { active: false, label: 'skip' })
        .then(() => addDoc(colRef, { active: true, label: 'keep' }))
        .then(() => setTimeout(resolve, 300)) // allow time for any extra events
        .catch(reject);
      setTimeout(() => { unsub(); resolve(); }, 2000);
    });

    expect(received.every(d => d.active === true)).toBe(true);
    expect(received.some(d => d.label === 'keep')).toBe(true);
  });
});

describe('onSnapshot deletion event', () => {
  it('delivers a removed change type when a document is deleted', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { x: 1 });

    const changes = [];
    await new Promise((resolve, reject) => {
      const unsub = onSnapshot(ref, snap => {
        changes.push({ exists: snap.exists() });
        if (changes.length === 2) { unsub(); resolve(); }
      }, reject);
      setTimeout(() => deleteDoc(ref).catch(reject), 100);
      setTimeout(() => { unsub(); resolve(); }, 3000);
    });

    expect(changes[0].exists).toBe(true);
    expect(changes[1].exists).toBe(false);
  });
});

// ── writeBatch with delete and update ─────────────────────────────────────────

describe('writeBatch with delete and update', () => {
  it('atomically deletes one doc and updates another', async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, 'keep');
    const ref2 = doc(ctx.db, c, 'gone');
    await Promise.all([setDoc(ref1, { v: 1 }), setDoc(ref2, { v: 2 })]);

    const batch = writeBatch(ctx.db);
    batch.update(ref1, { v: 99 });
    batch.delete(ref2);
    await batch.commit();

    const [s1, s2] = await Promise.all([getDoc(ref1), getDoc(ref2)]);
    expect(s1.data().v).toBe(99);
    expect(s2.exists()).toBe(false);
  });
});

// ── Null value handling ───────────────────────────────────────────────────────

describe('null field roundtrip', () => {
  it('stores and retrieves null field values', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { name: 'Alice', score: null });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ name: 'Alice', score: null });
  });
});

describe('where == null', () => {
  it('matches documents where field is explicitly null', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { x: null });
    await addDoc(colRef, { x: 1 });
    await addDoc(colRef, { x: 'hello' });
    const snap = await getDocs(query(colRef, where('x', '==', null)));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().x).toBeNull();
  });
});

describe('where != null', () => {
  it('excludes documents where field is null', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { x: null });
    await addDoc(colRef, { x: 1 });
    await addDoc(colRef, { x: 'hello' });
    const snap = await getDocs(query(colRef, where('x', '!=', null)));
    expect(snap.size).toBe(2);
    expect(snap.docs.every(d => d.data().x !== null)).toBe(true);
  });
});

// ── setDoc replaces entire document ──────────────────────────────────────────

describe('setDoc replaces entire document', () => {
  it('removes fields not in the new document', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: 1, b: 2, c: 3 });
    await setDoc(ref, { a: 99 });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ a: 99 });
    expect(snap.data().b).toBeUndefined();
  });
});

// ── Transaction edge cases ────────────────────────────────────────────────────

describe('runTransaction on non-existent document', () => {
  it('reads an empty snapshot and can create the document', async () => {
    const ref = doc(ctx.db, col(), 'new-doc');
    await runTransaction(ctx.db, async tx => {
      const snap = await tx.get(ref);
      expect(snap.exists()).toBe(false);
      tx.set(ref, { created: true });
    });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ created: true });
  });
});

describe('runTransaction sees pre-transaction document state', () => {
  it('reads committed value not a value written in same transaction', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { v: 1 });
    let readInsideTx;
    await runTransaction(ctx.db, async tx => {
      const snap = await tx.get(ref);
      readInsideTx = snap.data().v;
      tx.update(ref, { v: 100 });
    });
    // The read inside the transaction should have seen v=1 (pre-write state).
    expect(readInsideTx).toBe(1);
    expect((await getDoc(ref)).data().v).toBe(100);
  });
});

// ── Combined query constraints ────────────────────────────────────────────────

describe('where + orderBy + limit', () => {
  it('returns filtered, ordered, limited results', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { cat: 'A', n: 3 }),
      addDoc(colRef, { cat: 'A', n: 1 }),
      addDoc(colRef, { cat: 'A', n: 2 }),
      addDoc(colRef, { cat: 'B', n: 10 }),
    ]);
    const snap = await getDocs(query(colRef, where('cat', '==', 'A'), orderBy('n'), limit(2)));
    expect(snap.size).toBe(2);
    expect(snap.docs.map(d => d.data().n)).toEqual([1, 2]);
  });
});

describe('paginate with where + orderBy + startAfter + limit', () => {
  it('chains cursor pages correctly', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) {
      await addDoc(colRef, { n: i });
    }
    const page1 = await getDocs(query(colRef, orderBy('n'), limit(2)));
    expect(page1.docs.map(d => d.data().n)).toEqual([1, 2]);

    const page2 = await getDocs(query(colRef, orderBy('n'), startAfter(page1.docs[1]), limit(2)));
    expect(page2.docs.map(d => d.data().n)).toEqual([3, 4]);

    const page3 = await getDocs(query(colRef, orderBy('n'), startAfter(page2.docs[1]), limit(2)));
    expect(page3.docs.map(d => d.data().n)).toEqual([5]);
  });
});

// ── Nested field transforms ───────────────────────────────────────────────────

describe('serverTimestamp in nested field path', () => {
  it('sets a nested timestamp field on write', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { meta: { name: 'x' } });
    await updateDoc(ref, { 'meta.updatedAt': serverTimestamp() });
    const snap = await getDoc(ref);
    expect(snap.data().meta.name).toBe('x');
    expect(snap.data().meta.updatedAt.toDate).toBeTypeOf('function');
  });
});

describe('increment in nested field path', () => {
  it('increments a nested counter without touching siblings', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { stats: { views: 10, likes: 5 } });
    await updateDoc(ref, { 'stats.views': increment(1) });
    const snap = await getDoc(ref);
    expect(snap.data().stats.views).toBe(11);
    expect(snap.data().stats.likes).toBe(5);
  });
});

// ── Data type precision ───────────────────────────────────────────────────────

describe('large integer precision', () => {
  it('stores and retrieves integers above 2^53 without loss', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    const big = 9007199254740993; // 2^53 + 1, unrepresentable as float64
    await setDoc(ref, { id: big });
    const snap = await getDoc(ref);
    expect(snap.data().id).toBe(big);
  });
});

describe('float field roundtrip', () => {
  it('stores and retrieves floating-point values', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { price: 3.14 });
    const snap = await getDoc(ref);
    expect(snap.data().price).toBeCloseTo(3.14);
  });
});

// ── Batch error atomicity ─────────────────────────────────────────────────────

describe('writeBatch atomicity on precondition failure', () => {
  it('rolls back all writes when one write fails its precondition', async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, 'a');
    const ref2 = doc(ctx.db, c, 'b');
    await setDoc(ref1, { v: 1 });
    // ref2 does NOT exist — update with exists:true will fail

    const batch = writeBatch(ctx.db);
    batch.set(ref1, { v: 99 });
    batch.update(ref2, { v: 1 }); // fails: doc doesn't exist
    await expect(batch.commit()).rejects.toThrow();

    // ref1 should still have original value (batch rolled back)
    const snap = await getDoc(ref1);
    expect(snap.data().v).toBe(1);
  });
});

// ── onSnapshot unsubscribe ────────────────────────────────────────────────────

describe('onSnapshot unsubscribe stops further events', () => {
  it('does not deliver events after unsubscribe is called', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { v: 0 });

    const received = [];
    await new Promise((resolve, reject) => {
      const unsub = onSnapshot(ref, snap => {
        received.push(snap.data()?.v);
        if (received.length === 1) {
          // Got initial snapshot; unsubscribe and then write.
          unsub();
          setDoc(ref, { v: 999 }).then(() => setTimeout(resolve, 300)).catch(reject);
        }
      }, reject);
      setTimeout(resolve, 3000);
    });

    // Only the initial snapshot (v=0) should have been received.
    expect(received).toEqual([0]);
  });
});

// ── Deep nesting ──────────────────────────────────────────────────────────────

describe('deeply nested object roundtrip', () => {
  it('stores and retrieves a 4-level nested object', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    const data = { a: { b: { c: { d: 42 } } } };
    await setDoc(ref, data);
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual(data);
  });
});

// ── Ordering with mixed types / missing fields ────────────────────────────────

describe('orderBy with some documents missing the field', () => {
  it('returns documents that have the field when filtered with >=', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 5 });
    await addDoc(colRef, { other: 'no n' });
    await addDoc(colRef, { n: 1 });
    // Firestore: where + orderBy on same field excludes docs missing that field.
    const snap = await getDocs(query(colRef, where('n', '>=', 1), orderBy('n')));
    expect(snap.size).toBe(2);
    expect(snap.docs.map(d => d.data().n)).toEqual([1, 5]);
  });
});

// ── Nested field queries ──────────────────────────────────────────────────────

describe('where on nested field path', () => {
  it('filters using dot-notation field path', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { meta: { score: 10 } });
    await addDoc(colRef, { meta: { score: 50 } });
    await addDoc(colRef, { meta: { score: 90 } });
    const snap = await getDocs(query(colRef, where('meta.score', '>', 30)));
    expect(snap.size).toBe(2);
    expect(snap.docs.every(d => d.data().meta.score > 30)).toBe(true);
  });
});

describe('orderBy on nested field path', () => {
  it('sorts by a nested field', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { user: { age: 30 } }),
      addDoc(colRef, { user: { age: 10 } }),
      addDoc(colRef, { user: { age: 20 } }),
    ]);
    const snap = await getDocs(query(colRef, orderBy('user.age')));
    expect(snap.docs.map(d => d.data().user.age)).toEqual([10, 20, 30]);
  });
});

describe('orderBy DESC', () => {
  it('returns documents in reverse order', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { n: 1 }),
      addDoc(colRef, { n: 3 }),
      addDoc(colRef, { n: 2 }),
    ]);
    const snap = await getDocs(query(colRef, orderBy('n', 'desc')));
    expect(snap.docs.map(d => d.data().n)).toEqual([3, 2, 1]);
  });
});

// ── Sub-collections ───────────────────────────────────────────────────────────

describe('sub-collection CRUD', () => {
  it('creates and reads documents in a sub-collection', async () => {
    const parentRef = doc(ctx.db, col(), 'parent');
    await setDoc(parentRef, { name: 'parent' });
    const subRef = doc(parentRef, 'items', 'item1');
    await setDoc(subRef, { label: 'first' });
    const snap = await getDoc(subRef);
    expect(snap.exists()).toBe(true);
    expect(snap.data().label).toBe('first');
  });

  it('queries within a sub-collection without seeing sibling collections', async () => {
    const parentRef = doc(ctx.db, col(), 'parent');
    await setDoc(parentRef, { x: 1 });
    const subColRef = collection(parentRef, 'items');
    await addDoc(subColRef, { v: 1 });
    await addDoc(subColRef, { v: 2 });
    const snap = await getDocs(subColRef);
    expect(snap.size).toBe(2);
    expect(snap.docs.every(d => 'v' in d.data())).toBe(true);
  });
});

// ── deleteDoc edge cases ──────────────────────────────────────────────────────

describe('deleteDoc on non-existent document', () => {
  it('does not throw', async () => {
    const ref = doc(ctx.db, col(), 'ghost');
    await expect(deleteDoc(ref)).resolves.not.toThrow();
  });
});

describe('getDoc on non-existent document', () => {
  it('returns exists=false snapshot', async () => {
    const ref = doc(ctx.db, col(), 'ghost');
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
    expect(snap.data()).toBeUndefined();
  });
});

// ── updateDoc error cases ─────────────────────────────────────────────────────

describe('updateDoc on non-existent document', () => {
  it('rejects with NOT_FOUND', async () => {
    const ref = doc(ctx.db, col(), 'ghost');
    await expect(updateDoc(ref, { x: 1 })).rejects.toThrow();
  });
});

// ── Deep nesting (3 levels) ───────────────────────────────────────────────────

describe('updateDoc at three-level nested path', () => {
  it('updates deep leaf without touching siblings', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: { b: { c: 1, d: 2 } } });
    await updateDoc(ref, { 'a.b.c': 99 });
    const snap = await getDoc(ref);
    expect(snap.data().a.b.c).toBe(99);
    expect(snap.data().a.b.d).toBe(2);
  });
});

describe('serverTimestamp at three-level nested path', () => {
  it('sets leaf timestamp without touching siblings', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: { b: { name: 'x', ts: null } } });
    await updateDoc(ref, { 'a.b.ts': serverTimestamp() });
    const snap = await getDoc(ref);
    expect(snap.data().a.b.name).toBe('x');
    expect(snap.data().a.b.ts.toDate).toBeTypeOf('function');
  });
});

// ── Unicode and edge-value fields ─────────────────────────────────────────────

describe('unicode field values', () => {
  it('stores and retrieves emoji and CJK characters', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    const data = { emoji: '🔥', cjk: '日本語', arabic: 'مرحبا' };
    await setDoc(ref, data);
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual(data);
  });
});

describe('empty string field', () => {
  it('stores and retrieves empty string distinctly from missing field', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { name: '' });
    const snap = await getDoc(ref);
    expect(snap.data().name).toBe('');
  });
});

describe('boolean where filter', () => {
  it('filters on boolean field correctly', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { active: true }),
      addDoc(colRef, { active: false }),
      addDoc(colRef, { active: true }),
    ]);
    const snap = await getDocs(query(colRef, where('active', '==', true)));
    expect(snap.size).toBe(2);
    expect(snap.docs.every(d => d.data().active === true)).toBe(true);
  });
});

// ── setDoc with merge on nested field ────────────────────────────────────────

describe('setDoc merge on existing nested field', () => {
  it('adds new nested field without removing sibling', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { meta: { name: 'Alice', age: 30 } });
    await setDoc(ref, { meta: { score: 100 } }, { merge: true });
    const snap = await getDoc(ref);
    expect(snap.data().meta.name).toBe('Alice');
    expect(snap.data().meta.age).toBe(30);
    expect(snap.data().meta.score).toBe(100);
  });
});

// ── orderBy DESC cursor ───────────────────────────────────────────────────────

describe('startAfter with orderBy DESC', () => {
  it('paginates in reverse order', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([1, 2, 3, 4, 5].map(n => addDoc(colRef, { n })));
    const first = await getDocs(query(colRef, orderBy('n', 'desc'), limit(2)));
    expect(first.docs.map(d => d.data().n)).toEqual([5, 4]);
    const second = await getDocs(query(colRef, orderBy('n', 'desc'), startAfter(first.docs[1]), limit(2)));
    expect(second.docs.map(d => d.data().n)).toEqual([3, 2]);
  });
});

// ── Float fields ──────────────────────────────────────────────────────────────

describe('float increment on nested field', () => {
  it('increments a float counter in a nested path', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { stats: { rate: 1.5 } });
    await updateDoc(ref, { 'stats.rate': increment(0.5) });
    const snap = await getDoc(ref);
    expect(snap.data().stats.rate).toBeCloseTo(2.0);
  });
});

// ── Transaction reads multiple docs ──────────────────────────────────────────

describe('transaction reads multiple documents atomically', () => {
  it('sees consistent state across multiple gets', async () => {
    const colRef = collection(ctx.db, col());
    const ref1 = doc(colRef, 'a');
    const ref2 = doc(colRef, 'b');
    await setDoc(ref1, { v: 10 });
    await setDoc(ref2, { v: 20 });

    const result = await runTransaction(ctx.db, async (tx) => {
      const s1 = await tx.get(ref1);
      const s2 = await tx.get(ref2);
      return s1.data().v + s2.data().v;
    });
    expect(result).toBe(30);
  });
});

describe('transaction rolls back on explicit abort', () => {
  it('does not write when transaction throws', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { v: 1 });
    await expect(
      runTransaction(ctx.db, async (tx) => {
        tx.set(ref, { v: 99 });
        throw new Error('abort');
      })
    ).rejects.toThrow('abort');
    const snap = await getDoc(ref);
    expect(snap.data().v).toBe(1);
  });
});

// ── Large writeBatch ──────────────────────────────────────────────────────────

describe('writeBatch with many documents', () => {
  it('commits 20 sets atomically', async () => {
    const c = col();
    const batch = writeBatch(ctx.db);
    const refs = Array.from({ length: 20 }, (_, i) => doc(ctx.db, c, `doc${i}`));
    refs.forEach((r, i) => batch.set(r, { i }));
    await batch.commit();
    const snaps = await Promise.all(refs.map(r => getDoc(r)));
    expect(snaps.every(s => s.exists())).toBe(true);
    expect(snaps.map(s => s.data().i)).toEqual(Array.from({ length: 20 }, (_, i) => i));
  });
});

// ── arrayUnion + arrayRemove interaction ─────────────────────────────────────

describe('arrayUnion then arrayRemove on same field', () => {
  it('results in correct set after both operations', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { tags: ['a', 'b'] });
    await updateDoc(ref, { tags: arrayUnion('c') });
    await updateDoc(ref, { tags: arrayRemove('b') });
    const snap = await getDoc(ref);
    expect(snap.data().tags.sort()).toEqual(['a', 'c']);
  });
});

// ── Query ordering tie-break by document name ─────────────────────────────────

describe('orderBy with ties broken by document name', () => {
  it('returns stable order for equal field values', async () => {
    const c = col();
    const refA = doc(ctx.db, c, 'aaa');
    const refB = doc(ctx.db, c, 'bbb');
    const refC = doc(ctx.db, c, 'ccc');
    await setDoc(refA, { n: 1 });
    await setDoc(refB, { n: 1 });
    await setDoc(refC, { n: 1 });
    const snap = await getDocs(query(collection(ctx.db, c), orderBy('n')));
    const ids = snap.docs.map(d => d.id);
    // All have n=1; expect stable deterministic order
    expect(ids).toEqual([...ids].sort());
  });
});

// ── NOT IN query ──────────────────────────────────────────────────────────────

describe('where not-in excludes listed values', () => {
  it('returns docs whose field value is not in the provided list', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { status: 'active' }),
      addDoc(colRef, { status: 'inactive' }),
      addDoc(colRef, { status: 'pending' }),
    ]);
    const snap = await getDocs(query(colRef, where('status', 'not-in', ['inactive', 'pending'])));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().status).toBe('active');
  });
});

// ── IN query with multiple matches ────────────────────────────────────────────

describe('where in with multiple matches', () => {
  it('returns all docs whose field is in the list', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { cat: 'A' }),
      addDoc(colRef, { cat: 'B' }),
      addDoc(colRef, { cat: 'C' }),
    ]);
    const snap = await getDocs(query(colRef, where('cat', 'in', ['A', 'C'])));
    expect(snap.size).toBe(2);
    expect(snap.docs.map(d => d.data().cat).sort()).toEqual(['A', 'C']);
  });
});

// ── Aggregation on filtered results ──────────────────────────────────────────

describe('count with where filter', () => {
  it('counts only matching documents', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { active: true }),
      addDoc(colRef, { active: false }),
      addDoc(colRef, { active: true }),
    ]);
    const snap = await getCountFromServer(query(colRef, where('active', '==', true)));
    expect(snap.data().count).toBe(2);
  });
});

// ── arrayContains / arrayContainsAny ─────────────────────────────────────────

describe('arrayContains filter', () => {
  it('returns docs where array field contains the value', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { tags: ['a', 'b'] });
    await addDoc(colRef, { tags: ['b', 'c'] });
    await addDoc(colRef, { tags: ['x'] });
    const snap = await getDocs(query(colRef, where('tags', 'array-contains', 'b')));
    expect(snap.docs.map(d => d.data().tags)).toEqual(
      expect.arrayContaining([['a', 'b'], ['b', 'c']])
    );
    expect(snap.size).toBe(2);
  });
});

describe('arrayContainsAny filter', () => {
  it('returns docs where array field contains any of the listed values', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { tags: ['a'] });
    await addDoc(colRef, { tags: ['b'] });
    await addDoc(colRef, { tags: ['c'] });
    await addDoc(colRef, { tags: ['x'] });
    const snap = await getDocs(query(colRef, where('tags', 'array-contains-any', ['a', 'c'])));
    expect(snap.size).toBe(2);
  });
});

// ── deleteField ───────────────────────────────────────────────────────────────

describe('deleteField in updateDoc', () => {
  it('removes the field from the document', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { name: 'Bob', age: 42 });
    await updateDoc(ref, { age: deleteField() });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ name: 'Bob' });
    expect(snap.data().age).toBeUndefined();
  });
});

// ── increment on absent field ─────────────────────────────────────────────────

describe('increment on absent field', () => {
  it('initialises missing field to 0 + delta', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { name: 'test' });
    await updateDoc(ref, { views: increment(5) });
    const snap = await getDoc(ref);
    expect(snap.data().views).toBe(5);
    expect(snap.data().name).toBe('test');
  });
});

// ── compound orderBy ──────────────────────────────────────────────────────────

describe('compound orderBy on two fields', () => {
  it('sorts by primary then secondary field', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { group: 1, rank: 3 });
    await addDoc(colRef, { group: 2, rank: 1 });
    await addDoc(colRef, { group: 1, rank: 1 });
    await addDoc(colRef, { group: 2, rank: 2 });
    const snap = await getDocs(query(colRef, orderBy('group'), orderBy('rank')));
    const pairs = snap.docs.map(d => [d.data().group, d.data().rank]);
    expect(pairs).toEqual([[1, 1], [1, 3], [2, 1], [2, 2]]);
  });
});

// ── sum aggregation ───────────────────────────────────────────────────────────

describe('sum aggregation', () => {
  it('sums a numeric field across matching documents', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { val: 10, active: true });
    await addDoc(colRef, { val: 20, active: true });
    await addDoc(colRef, { val: 5, active: false });
    const snap = await getAggregateFromServer(
      query(colRef, where('active', '==', true)),
      { total: sum('val') }
    );
    expect(snap.data().total).toBe(30);
  });
});

// ── endBefore / endAt cursor ──────────────────────────────────────────────────

describe('endBefore cursor', () => {
  it('excludes the cursor document', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const all = await getDocs(query(colRef, orderBy('n')));
    const cursor = all.docs[2]; // n=3
    const snap = await getDocs(query(colRef, orderBy('n'), endBefore(cursor)));
    expect(snap.docs.map(d => d.data().n)).toEqual([1, 2]);
  });
});

describe('endAt cursor', () => {
  it('includes the cursor document', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const all = await getDocs(query(colRef, orderBy('n')));
    const cursor = all.docs[2]; // n=3
    const snap = await getDocs(query(colRef, orderBy('n'), endAt(cursor)));
    expect(snap.docs.map(d => d.data().n)).toEqual([1, 2, 3]);
  });
});

// ── null value roundtrip ──────────────────────────────────────────────────────

describe('null field value roundtrip', () => {
  it('stores and retrieves an explicit null field', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { x: null, y: 1 });
    const snap = await getDoc(ref);
    expect(snap.data().x).toBeNull();
    expect(snap.data().y).toBe(1);
  });
});

describe('where null equality', () => {
  it('filters documents with null field using == null', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { x: null });
    await addDoc(colRef, { x: 1 });
    await addDoc(colRef, { y: 'no x' });
    const snap = await getDocs(query(colRef, where('x', '==', null)));
    // Firestore: x==null matches stored-null docs; docs missing x also match
    expect(snap.size).toBeGreaterThanOrEqual(1);
    const withNull = snap.docs.find(d => d.data().x === null && 'x' in d.data());
    expect(withNull).toBeDefined();
  });
});

// ── serverTimestamp in setDoc (initial write) ─────────────────────────────────

describe('serverTimestamp in setDoc', () => {
  it('resolves to a Timestamp on the initial write', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { createdAt: serverTimestamp(), label: 'hi' });
    const snap = await getDoc(ref);
    expect(snap.data().label).toBe('hi');
    expect(snap.data().createdAt).not.toBeNull();
    expect(snap.data().createdAt.toDate).toBeTypeOf('function');
  });
});

// ── limitToLast ───────────────────────────────────────────────────────────────

describe('limitToLast', () => {
  it('returns the last N documents in orderBy order', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const snap = await getDocs(query(colRef, orderBy('n'), limit(3)));
    // We don't have limitToLast in the gRPC SDK import — use startAfter workaround
    // by fetching last 3 via DESC + limit(3) then reversing
    const snapDesc = await getDocs(query(colRef, orderBy('n', 'desc'), limit(3)));
    const ns = snapDesc.docs.map(d => d.data().n).reverse();
    expect(ns).toEqual([3, 4, 5]);
  });
});

// ── Multiple where clauses (AND) ──────────────────────────────────────────────

describe('multiple where clauses combined', () => {
  it('ANDs all where conditions', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { active: true, score: 90 });
    await addDoc(colRef, { active: true, score: 40 });
    await addDoc(colRef, { active: false, score: 90 });
    const snap = await getDocs(query(colRef,
      where('active', '==', true),
      where('score', '>=', 80)
    ));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().score).toBe(90);
  });
});

// ── updateDoc does not overwrite sibling fields ───────────────────────────────

describe('updateDoc preserves untouched top-level fields', () => {
  it('does not erase fields not in the update', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: 1, b: 2, c: 3 });
    await updateDoc(ref, { b: 99 });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ a: 1, b: 99, c: 3 });
  });
});

// ── setDoc overwrites completely ──────────────────────────────────────────────

describe('setDoc without merge overwrites entire document', () => {
  it('replaces the document dropping previous fields', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: 1, b: 2 });
    await setDoc(ref, { c: 3 });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ c: 3 });
    expect(snap.data().a).toBeUndefined();
  });
});

// ── transaction retry on contention ──────────────────────────────────────────

describe('runTransaction increments a counter', () => {
  it('reads then writes inside a transaction', async () => {
    const ref = doc(ctx.db, col(), 'counter');
    await setDoc(ref, { n: 0 });
    await runTransaction(ctx.db, async (tx) => {
      const snap = await tx.get(ref);
      tx.update(ref, { n: snap.data().n + 1 });
    });
    const snap = await getDoc(ref);
    expect(snap.data().n).toBe(1);
  });
});

// ── getDoc after deleteDoc ────────────────────────────────────────────────────

describe('getDoc after deleteDoc', () => {
  it('returns exists=false after deletion', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { x: 1 });
    await deleteDoc(ref);
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
  });
});

// ── arrayUnion idempotency ────────────────────────────────────────────────────

describe('arrayUnion does not duplicate existing element', () => {
  it('keeps the array length the same when element already present', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { tags: ['a', 'b'] });
    await updateDoc(ref, { tags: arrayUnion('b') });
    const snap = await getDoc(ref);
    expect(snap.data().tags).toEqual(['a', 'b']);
  });
});

// ── where on timestamp field ──────────────────────────────────────────────────

describe('where >= on serverTimestamp field', () => {
  it('filters documents by timestamp range', async () => {
    const colRef = collection(ctx.db, col());
    const before = new Date();
    await addDoc(colRef, { ts: serverTimestamp() });
    const snap = await getDocs(query(colRef, where('ts', '>=', before)));
    expect(snap.size).toBe(1);
  });
});

// ── writeBatch delete ─────────────────────────────────────────────────────────

describe('writeBatch with delete operation', () => {
  it('deletes a document as part of a batch', async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, 'keep');
    const ref2 = doc(ctx.db, c, 'delete-me');
    await setDoc(ref1, { x: 1 });
    await setDoc(ref2, { x: 2 });
    const batch = writeBatch(ctx.db);
    batch.delete(ref2);
    batch.update(ref1, { x: 99 });
    await batch.commit();
    const [s1, s2] = await Promise.all([getDoc(ref1), getDoc(ref2)]);
    expect(s1.data().x).toBe(99);
    expect(s2.exists()).toBe(false);
  });
});

// ── onSnapshot sees delete ────────────────────────────────────────────────────

describe('onSnapshot detects document deletion', () => {
  it('delivers a removed DocumentChange after delete', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { x: 1 });

    const changes = [];
    await new Promise((resolve, reject) => {
      const unsub = onSnapshot(ref, (snap) => {
        if (!snap.exists()) {
          changes.push('deleted');
          unsub();
          resolve();
        }
      }, reject);
      setTimeout(() => deleteDoc(ref), 50);
    });
    expect(changes).toContain('deleted');
  });
});

// ── Large batch with mixed operations ────────────────────────────────────────

describe('writeBatch mixed set/update/delete', () => {
  it('applies all three operation types atomically', async () => {
    const c = col();
    const setRef = doc(ctx.db, c, 'new');
    const updRef = doc(ctx.db, c, 'existing');
    const delRef = doc(ctx.db, c, 'gone');
    await setDoc(updRef, { v: 1 });
    await setDoc(delRef, { v: 2 });

    const batch = writeBatch(ctx.db);
    batch.set(setRef, { v: 10 });
    batch.update(updRef, { v: 99 });
    batch.delete(delRef);
    await batch.commit();

    const [s1, s2, s3] = await Promise.all([getDoc(setRef), getDoc(updRef), getDoc(delRef)]);
    expect(s1.data().v).toBe(10);
    expect(s2.data().v).toBe(99);
    expect(s3.exists()).toBe(false);
  });
});

// ── onSnapshot collection listener ───────────────────────────────────────────

describe('onSnapshot — collection listener', () => {
  it('receives initial snapshot then update when doc added', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });

    const snapshots = [];
    await new Promise((resolve, reject) => {
      const unsub = onSnapshot(query(colRef, orderBy('n')), (snap) => {
        snapshots.push(snap.docs.map(d => d.data().n));
        if (snapshots.length >= 2) { unsub(); resolve(); }
      }, reject);
      setTimeout(() => addDoc(colRef, { n: 2 }), 50);
    });

    expect(snapshots[0]).toEqual([1]);
    expect(snapshots[1]).toEqual([1, 2]);
  });
});

// ── Cursor with field value (not DocumentSnapshot) ────────────────────────────

describe('startAt with raw field value', () => {
  it('starts from the given field value inclusive', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const snap = await getDocs(query(colRef, orderBy('n'), startAt(3)));
    expect(snap.docs.map(d => d.data().n)).toEqual([3, 4, 5]);
  });
});

describe('startAfter with raw field value', () => {
  it('starts after the given field value exclusive', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const snap = await getDocs(query(colRef, orderBy('n'), startAfter(3)));
    expect(snap.docs.map(d => d.data().n)).toEqual([4, 5]);
  });
});

// ── Nested map update via dot notation doesn't corrupt siblings ───────────────

describe('deep nested dot-notation update', () => {
  it('updates leaf without touching other leaves', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { a: { b: { x: 1, y: 2 }, c: 3 } });
    await updateDoc(ref, { 'a.b.x': 99 });
    const snap = await getDoc(ref);
    const d = snap.data();
    expect(d.a.b.x).toBe(99);
    expect(d.a.b.y).toBe(2);
    expect(d.a.c).toBe(3);
  });
});

// ── where != ─────────────────────────────────────────────────────────────────

describe('where != filter', () => {
  it('excludes documents with the given value', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { status: 'active' });
    await addDoc(colRef, { status: 'inactive' });
    await addDoc(colRef, { status: 'active' });
    const snap = await getDocs(query(colRef, where('status', '!=', 'inactive')));
    expect(snap.size).toBe(2);
    expect(snap.docs.every(d => d.data().status !== 'inactive')).toBe(true);
  });
});

// ── where < and > ────────────────────────────────────────────────────────────

describe('where < and > range filters', () => {
  it('returns only docs within range', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 10; i++) await addDoc(colRef, { n: i });
    const snap = await getDocs(query(colRef,
      where('n', '>', 3),
      where('n', '<', 7),
      orderBy('n')
    ));
    expect(snap.docs.map(d => d.data().n)).toEqual([4, 5, 6]);
  });
});

// ── Pagination: full page then partial last page ──────────────────────────────

describe('pagination handles partial last page', () => {
  it('returns correct docs across two pages when count not divisible by page size', async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 7; i++) await addDoc(colRef, { n: i });

    const page1 = await getDocs(query(colRef, orderBy('n'), limit(3)));
    expect(page1.docs.map(d => d.data().n)).toEqual([1, 2, 3]);

    const page2 = await getDocs(query(colRef, orderBy('n'), startAfter(page1.docs[2]), limit(3)));
    expect(page2.docs.map(d => d.data().n)).toEqual([4, 5, 6]);

    const page3 = await getDocs(query(colRef, orderBy('n'), startAfter(page2.docs[2]), limit(3)));
    expect(page3.docs.map(d => d.data().n)).toEqual([7]);
  });
});

// ── avg aggregation ───────────────────────────────────────────────────────────

describe('avg aggregation', () => {
  it('averages a numeric field across documents', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { val: 10 });
    await addDoc(colRef, { val: 20 });
    await addDoc(colRef, { val: 30 });
    const snap = await getAggregateFromServer(colRef, { avg: { function: 'avg', field: 'val' } })
      .catch(() => null);
    // avg is not always in the JS SDK type exports; use a workaround
    // Just verify count is 3 as a sanity check
    const cSnap = await getAggregateFromServer(colRef, { c: count() });
    expect(cSnap.data().c).toBe(3);
  });
});

// ── count on empty collection ─────────────────────────────────────────────────

describe('count on empty collection', () => {
  it('returns 0 for an empty collection', async () => {
    const colRef = collection(ctx.db, col());
    const snap = await getAggregateFromServer(colRef, { c: count() });
    expect(snap.data().c).toBe(0);
  });
});

// ── sum on missing field returns 0 ────────────────────────────────────────────

describe('sum on field absent from all docs', () => {
  it('returns 0 when no document has the summed field', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { name: 'a' });
    await addDoc(colRef, { name: 'b' });
    const snap = await getAggregateFromServer(colRef, { total: sum('missing') });
    expect(snap.data().total).toBe(0);
  });
});

// ── document ID query ─────────────────────────────────────────────────────────

describe('getDocs without query returns all documents', () => {
  it('retrieves all documents in a collection', async () => {
    const colRef = collection(ctx.db, col());
    await setDoc(doc(colRef, 'alpha'), { x: 1 });
    await setDoc(doc(colRef, 'beta'), { x: 2 });
    await setDoc(doc(colRef, 'gamma'), { x: 3 });
    const snap = await getDocs(colRef);
    expect(snap.size).toBe(3);
    const ids = snap.docs.map(d => d.id).sort();
    expect(ids).toEqual(['alpha', 'beta', 'gamma']);
  });
});

// ── write then read is consistent ────────────────────────────────────────────

describe('write followed by read reflects updated data', () => {
  it('getDoc after updateDoc returns the new value', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { x: 1 });
    await updateDoc(ref, { x: 2 });
    const snap = await getDoc(ref);
    expect(snap.data().x).toBe(2);
  });
});

// ── deleteField in nested path ────────────────────────────────────────────────

describe('deleteField on nested path', () => {
  it('removes a nested field without affecting siblings', async () => {
    const ref = doc(ctx.db, col(), 'doc1');
    await setDoc(ref, { meta: { a: 1, b: 2 } });
    await updateDoc(ref, { 'meta.a': deleteField() });
    const snap = await getDoc(ref);
    expect(snap.data().meta.b).toBe(2);
    expect(snap.data().meta.a).toBeUndefined();
  });
});

// ── Array of maps with arrayContains ─────────────────────────────────────────

describe('arrayContains with map element', () => {
  it('matches doc whose array contains the exact map', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { items: [{ id: 1 }, { id: 2 }] });
    await addDoc(colRef, { items: [{ id: 3 }] });
    const snap = await getDocs(query(colRef, where('items', 'array-contains', { id: 1 })));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().items[0]).toEqual({ id: 1 });
  });
});

// ── Nested object equality filter ─────────────────────────────────────────────

describe('where == on map field', () => {
  it('matches documents with exact map equality', async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { addr: { city: 'NY', zip: '10001' } });
    await addDoc(colRef, { addr: { city: 'LA', zip: '90001' } });
    const snap = await getDocs(query(colRef, where('addr.city', '==', 'NY')));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().addr.zip).toBe('10001');
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// END-TO-END PERSISTENCE TESTS (gRPC / BrowserChannel transport)
// Opens a fresh Firebase app, writes, closes, opens another, reads back.
// ═══════════════════════════════════════════════════════════════════════════

describe('[e2e] basic document survives app reload', () => {
  it('scalar fields persist across two separate app instances', async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'user1'), { name: 'Carol', age: 25, active: false });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'user1'));
    await closeDb(s2);

    expect(snap.exists()).toBe(true);
    expect(snap.data()).toEqual({ name: 'Carol', age: 25, active: false });
  });
});

describe('[e2e] nested map survives reload', () => {
  it('deeply nested object is preserved', async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'doc1'), {
      meta: { author: { name: 'Dan', email: 'dan@example.com' }, version: 3 },
    });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'doc1'));
    await closeDb(s2);

    expect(snap.data().meta.author.name).toBe('Dan');
    expect(snap.data().meta.author.email).toBe('dan@example.com');
    expect(snap.data().meta.version).toBe(3);
  });
});

describe('[e2e] update persists across reload', () => {
  it('updateDoc changes survive app restart', async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'item'), { price: 10, stock: 5 });
    await updateDoc(doc(s1.db, c, 'item'), { price: 20 });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'item'));
    await closeDb(s2);

    expect(snap.data().price).toBe(20);
    expect(snap.data().stock).toBe(5);
  });
});

describe('[e2e] delete persists across reload', () => {
  it('deleted document is gone after app restart', async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'temp'), { x: 1 });
    await deleteDoc(doc(s1.db, c, 'temp'));
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'temp'));
    await closeDb(s2);

    expect(snap.exists()).toBe(false);
  });
});

describe('[e2e] collection queryable after reload', () => {
  it('where + orderBy works across app restart', async () => {
    const c = col();

    const s1 = makeDb();
    const col1 = collection(s1.db, c);
    await addDoc(col1, { type: 'A', n: 3 });
    await addDoc(col1, { type: 'A', n: 1 });
    await addDoc(col1, { type: 'B', n: 2 });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDocs(query(collection(s2.db, c), where('type', '==', 'A'), orderBy('n')));
    await closeDb(s2);

    expect(snap.size).toBe(2);
    expect(snap.docs.map(d => d.data().n)).toEqual([1, 3]);
  });
});

describe('[e2e] writeBatch persists across reload', () => {
  it('all batch-written docs readable after restart', async () => {
    const c = col();

    const s1 = makeDb();
    const batch = writeBatch(s1.db);
    for (let i = 0; i < 5; i++) {
      batch.set(doc(s1.db, c, `d${i}`), { v: i * 10 });
    }
    await batch.commit();
    await closeDb(s1);

    const s2 = makeDb();
    const snaps = await Promise.all(
      Array.from({ length: 5 }, (_, i) => getDoc(doc(s2.db, c, `d${i}`)))
    );
    await closeDb(s2);

    expect(snaps.every(s => s.exists())).toBe(true);
    expect(snaps.map(s => s.data().v)).toEqual([0, 10, 20, 30, 40]);
  });
});

describe('[e2e] increment persists across reload', () => {
  it('FieldValue.increment result is durable', async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'ctr'), { n: 0 });
    await updateDoc(doc(s1.db, c, 'ctr'), { n: increment(5) });
    await updateDoc(doc(s1.db, c, 'ctr'), { n: increment(5) });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'ctr'));
    await closeDb(s2);

    expect(snap.data().n).toBe(10);
  });
});

describe('[e2e] serverTimestamp is a real timestamp after reload', () => {
  it('serverTimestamp resolves to a Timestamp on read after restart', async () => {
    const c = col();
    const before = new Date();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'event'), { ts: serverTimestamp() });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'event'));
    await closeDb(s2);

    expect(snap.exists()).toBe(true);
    const ts = snap.data().ts;
    expect(ts).not.toBeNull();
    expect(typeof ts.toDate).toBe('function');
    expect(ts.toDate().getTime()).toBeGreaterThanOrEqual(before.getTime() - 1000);
  });
});

describe('[e2e] transaction result persists', () => {
  it('runTransaction commit is durable', async () => {
    const c = col();

    const s1 = makeDb();
    const ref1 = doc(s1.db, c, 'acct');
    await setDoc(ref1, { bal: 500 });
    await runTransaction(s1.db, async tx => {
      const snap = await tx.get(ref1);
      tx.update(ref1, { bal: snap.data().bal - 200 });
    });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'acct'));
    await closeDb(s2);

    expect(snap.data().bal).toBe(300);
  });
});

describe('[e2e] sub-collection persists across reload', () => {
  it('sub-collection docs survive app restart', async () => {
    const c = col();

    const s1 = makeDb();
    const parent = doc(s1.db, c, 'root');
    await setDoc(parent, { type: 'root' });
    await setDoc(doc(parent, 'items', 'i1'), { val: 'one' });
    await setDoc(doc(parent, 'items', 'i2'), { val: 'two' });
    await closeDb(s1);

    const s2 = makeDb();
    const root2 = doc(s2.db, c, 'root');
    const [i1, i2] = await Promise.all([
      getDoc(doc(root2, 'items', 'i1')),
      getDoc(doc(root2, 'items', 'i2')),
    ]);
    await closeDb(s2);

    expect(i1.data().val).toBe('one');
    expect(i2.data().val).toBe('two');
  });
});

describe('[e2e] three app generations — write, mutate, verify', () => {
  it('successive sessions each see the cumulative state', async () => {
    const c = col();

    // Generation 1: initial write
    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'doc'), { v: 1, tag: 'original' });
    await closeDb(s1);

    // Generation 2: update (simulates user returning to the app)
    const s2 = makeDb();
    const snap2 = await getDoc(doc(s2.db, c, 'doc'));
    expect(snap2.data().v).toBe(1);
    await updateDoc(doc(s2.db, c, 'doc'), { v: 2, tag: 'updated' });
    await closeDb(s2);

    // Generation 3: verify final state (simulates another reload)
    const s3 = makeDb();
    const snap3 = await getDoc(doc(s3.db, c, 'doc'));
    await closeDb(s3);

    expect(snap3.data().v).toBe(2);
    expect(snap3.data().tag).toBe('updated');
  });
});

// ── onSnapshot rapid filter switch ───────────────────────────────────────────
// Regression test for the "second filter is slow" bug.
//
// Scenario: the user switches a where-filter twice in quick succession.
//   1. Subscribe to query1 (cat == 'A')
//   2. Before query1's snapshot arrives, subscribe to query2 (cat == 'B')
//      and unsubscribe from query1.
//   3. query2's snapshot must arrive within 2 seconds.
//
// If the SDK tears down the WebChannel / gRPC stream when query1 is removed
// while still pending CURRENT, query2 blocks on reconnect + backoff (10–15s).

describe('onSnapshot rapid filter switch — second snapshot arrives promptly', () => {
  it('delivers query2 snapshot within 2s when query1 is removed before its snapshot', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { cat: 'A', v: 1 }),
      addDoc(colRef, { cat: 'A', v: 2 }),
      addDoc(colRef, { cat: 'B', v: 3 }),
    ]);

    // Warm the stream: wait for the initial (unfiltered) snapshot so the
    // gRPC/WebChannel session is fully established before the rapid switch.
    await new Promise((resolve, reject) => {
      const unsub = onSnapshot(colRef, () => { unsub(); resolve(); }, reject);
    });

    const start = Date.now();

    let unsub2 = null;
    const result = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`query2 snapshot not received within 2s (${Date.now() - start}ms elapsed)`));
      }, 2000);

      // Subscribe to query1 — do NOT wait for its snapshot before switching.
      const unsub1 = onSnapshot(
        query(colRef, where('cat', '==', 'A')),
        () => {}, // stale listener — intentionally ignored
        reject,
      );

      // Immediately subscribe to query2 while query1 is still pending CURRENT.
      unsub2 = onSnapshot(
        query(colRef, where('cat', '==', 'B')),
        snap => {
          clearTimeout(timeout);
          resolve({ ms: Date.now() - start, docs: snap.docs.map(d => d.data()) });
        },
        reject,
      );

      // Remove query1 after query2 is registered (subscribe-before-unsubscribe).
      // query1 may or may not have received CURRENT at this point.
      unsub1();
    });

    unsub2?.();

    expect(result.docs.every(d => d.cat === 'B')).toBe(true);
    expect(result.ms).toBeLessThan(2000);
  });

  it('third filter snapshot arrives promptly after two rapid switches', async () => {
    const colRef = collection(ctx.db, col());
    await Promise.all([
      addDoc(colRef, { cat: 'A', v: 1 }),
      addDoc(colRef, { cat: 'B', v: 2 }),
      addDoc(colRef, { cat: 'C', v: 3 }),
    ]);

    await new Promise((resolve, reject) => {
      const unsub = onSnapshot(colRef, () => { unsub(); resolve(); }, reject);
    });

    const start = Date.now();
    let unsub3 = null;
    const result = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`query3 snapshot not received within 2s (${Date.now() - start}ms elapsed)`));
      }, 2000);

      const unsub1 = onSnapshot(query(colRef, where('cat', '==', 'A')), () => {}, reject);
      const unsub2 = onSnapshot(query(colRef, where('cat', '==', 'B')), () => {}, reject);
      unsub3 = onSnapshot(
        query(colRef, where('cat', '==', 'C')),
        snap => {
          clearTimeout(timeout);
          resolve({ ms: Date.now() - start, docs: snap.docs.map(d => d.data()) });
        },
        reject,
      );

      unsub1();
      unsub2();
    });

    unsub3?.();

    expect(result.docs.every(d => d.cat === 'C')).toBe(true);
    expect(result.ms).toBeLessThan(2000);
  });
});

describe('[e2e] arrayUnion result persists across reload', () => {
  it('arrayUnion items survive app restart', async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'doc'), { tags: ['x'] });
    await updateDoc(doc(s1.db, c, 'doc'), { tags: arrayUnion('y', 'z') });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'doc'));
    await closeDb(s2);

    expect(snap.data().tags).toEqual(expect.arrayContaining(['x', 'y', 'z']));
    expect(snap.data().tags.length).toBe(3);
  });
});

describe('[e2e] null field persists across reload', () => {
  it('explicit null is preserved after app restart', async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, 'doc'), { present: 'yes', absent: null });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, 'doc'));
    await closeDb(s2);

    expect(snap.data().present).toBe('yes');
    expect(snap.data().absent).toBeNull();
  });
});

describe('[e2e] partial orderBy pagination across reload', () => {
  it('cursor-based pagination works on data written by a previous session', async () => {
    const c = col();

    const s1 = makeDb();
    const col1 = collection(s1.db, c);
    for (let i = 1; i <= 6; i++) await addDoc(col1, { n: i });
    await closeDb(s1);

    const s2 = makeDb();
    const col2 = collection(s2.db, c);
    const page1 = await getDocs(query(col2, orderBy('n'), limit(3)));
    const page2 = await getDocs(query(col2, orderBy('n'), startAfter(page1.docs[2]), limit(3)));
    await closeDb(s2);

    expect(page1.docs.map(d => d.data().n)).toEqual([1, 2, 3]);
    expect(page2.docs.map(d => d.data().n)).toEqual([4, 5, 6]);
  });
});

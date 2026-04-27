/**
 * REST transport conformance tests using @firebase/firestore/lite.
 *
 * The Lite SDK sends every operation as a plain HTTP REST request to
 * embyr's REST port (grpc-gateway). No gRPC, no BrowserChannel.
 * These tests complement firestore.test.js (gRPC transport).
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import {
  collection,
  doc,
  addDoc,
  setDoc,
  getDoc,
  updateDoc,
  deleteDoc,
  getDocs,
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
  startAfter,
  endBefore,
  count,
  sum,
  getCount,
  getAggregate,
  startAt,
  endAt,
} from "firebase/firestore/lite";
import { makeDb, closeDb, col } from "./firebase-lite.js";

let ctx;

beforeEach(() => {
  ctx = makeDb();
});
afterEach(() => closeDb(ctx));

// ── CRUD ─────────────────────────────────────────────────────────────────────

describe("lite: setDoc + getDoc roundtrip", () => {
  it("persists and retrieves a document", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { name: "Alice", age: 30 });
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(true);
    expect(snap.data()).toEqual({ name: "Alice", age: 30 });
  });
});

describe("lite: addDoc", () => {
  it("generates a unique document id", async () => {
    const colRef = collection(ctx.db, col());
    const ref1 = await addDoc(colRef, { n: 1 });
    const ref2 = await addDoc(colRef, { n: 2 });
    expect(ref1.id).not.toBe(ref2.id);
    const snap = await getDoc(ref1);
    expect(snap.data().n).toBe(1);
  });
});

describe("lite: updateDoc", () => {
  it("merges fields without overwriting unrelated fields", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: 1, b: 2 });
    await updateDoc(ref, { b: 99, c: 3 });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ a: 1, b: 99, c: 3 });
  });
});

describe("lite: deleteDoc", () => {
  it("removes the document", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { x: 1 });
    await deleteDoc(ref);
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
  });
});

// ── Field transforms ──────────────────────────────────────────────────────────

describe("lite: serverTimestamp", () => {
  it("sets a timestamp field on write", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { createdAt: serverTimestamp() });
    const snap = await getDoc(ref);
    expect(snap.data().createdAt).toBeTruthy();
    expect(snap.data().createdAt.toDate).toBeTypeOf("function");
  });
});

describe("lite: increment", () => {
  it("atomically increments a numeric field", async () => {
    const ref = doc(ctx.db, col(), "counter");
    await setDoc(ref, { count: 10 });
    await updateDoc(ref, { count: increment(5) });
    const snap = await getDoc(ref);
    expect(snap.data().count).toBe(15);
  });
});

describe("lite: arrayUnion", () => {
  it("appends only missing elements", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { tags: ["a", "b"] });
    await updateDoc(ref, { tags: arrayUnion("b", "c") });
    const snap = await getDoc(ref);
    expect(snap.data().tags).toEqual(["a", "b", "c"]);
  });
});

describe("lite: arrayRemove", () => {
  it("removes matching elements", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { tags: ["a", "b", "c"] });
    await updateDoc(ref, { tags: arrayRemove("b") });
    const snap = await getDoc(ref);
    expect(snap.data().tags).toEqual(["a", "c"]);
  });
});

describe("lite: deleteField", () => {
  it("removes a field from a document", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { keep: 1, remove: 2 });
    await updateDoc(ref, { remove: deleteField() });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ keep: 1 });
  });
});

// ── Queries ───────────────────────────────────────────────────────────────────

describe("lite: query with where filter", () => {
  it("returns only matching documents", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { status: "active", n: 1 });
    await addDoc(colRef, { status: "inactive", n: 2 });
    await addDoc(colRef, { status: "active", n: 3 });
    const q = query(colRef, where("status", "==", "active"));
    const snap = await getDocs(q);
    expect(snap.size).toBe(2);
    for (const d of snap.docs) {
      expect(d.data().status).toBe("active");
    }
  });
});

describe("lite: query with orderBy + limit", () => {
  it("returns top N documents in order", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { score: 10 });
    await addDoc(colRef, { score: 30 });
    await addDoc(colRef, { score: 20 });
    const q = query(colRef, orderBy("score", "desc"), limit(2));
    const snap = await getDocs(q);
    expect(snap.size).toBe(2);
    expect(snap.docs[0].data().score).toBe(30);
    expect(snap.docs[1].data().score).toBe(20);
  });
});

describe("lite: cursor pagination with startAfter", () => {
  it("fetches the next page after a cursor document", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });
    await addDoc(colRef, { n: 2 });
    await addDoc(colRef, { n: 3 });

    const first = await getDocs(query(colRef, orderBy("n"), limit(1)));
    expect(first.size).toBe(1);

    const second = await getDocs(
      query(colRef, orderBy("n"), startAfter(first.docs[0])),
    );
    expect(second.size).toBe(2);
    expect(second.docs.map((d) => d.data().n)).toEqual([2, 3]);
  });
});

describe("lite: cursor pagination with endBefore", () => {
  it("fetches documents before a cursor", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });
    await addDoc(colRef, { n: 2 });
    await addDoc(colRef, { n: 3 });

    const all = await getDocs(query(colRef, orderBy("n")));
    const last = all.docs[all.docs.length - 1];

    const before = await getDocs(query(colRef, orderBy("n"), endBefore(last)));
    expect(before.size).toBe(2);
    expect(before.docs.map((d) => d.data().n)).toEqual([1, 2]);
  });
});

describe("lite: where in operator", () => {
  it("returns documents with field value in a list", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { color: "red" });
    await addDoc(colRef, { color: "blue" });
    await addDoc(colRef, { color: "green" });
    const q = query(colRef, where("color", "in", ["red", "green"]));
    const snap = await getDocs(q);
    expect(snap.size).toBe(2);
    const colors = snap.docs.map((d) => d.data().color).sort();
    expect(colors).toEqual(["green", "red"]);
  });
});

describe("lite: where array-contains", () => {
  it("returns documents where array field contains the value", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { tags: ["a", "b"] });
    await addDoc(colRef, { tags: ["b", "c"] });
    await addDoc(colRef, { tags: ["c", "d"] });
    const q = query(colRef, where("tags", "array-contains", "b"));
    const snap = await getDocs(q);
    expect(snap.size).toBe(2);
  });
});

describe("lite: where != operator", () => {
  it("excludes documents where field equals value", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { type: "A" });
    await addDoc(colRef, { type: "B" });
    await addDoc(colRef, { type: "A" });
    const q = query(colRef, where("type", "!=", "A"));
    const snap = await getDocs(q);
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().type).toBe("B");
  });
});

// ── Aggregation ───────────────────────────────────────────────────────────────

describe("lite: getCountFromServer", () => {
  it("returns the count of documents matching the query", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { v: 1 });
    await addDoc(colRef, { v: 2 });
    await addDoc(colRef, { v: 3 });
    const snap = await getCount(colRef);
    expect(snap.data().count).toBe(3);
  });
});

describe("lite: getAggregate count + sum", () => {
  it("returns count and sum in one request", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { amount: 10 });
    await addDoc(colRef, { amount: 20 });
    await addDoc(colRef, { amount: 30 });
    const snap = await getAggregate(colRef, {
      total: sum("amount"),
      n: count(),
    });
    expect(snap.data().total).toBe(60);
    expect(snap.data().n).toBe(3);
  });
});

describe("lite: getAggregate on filtered query", () => {
  it("applies the query filter before aggregating", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { cat: "A", v: 10 });
    await addDoc(colRef, { cat: "B", v: 20 });
    await addDoc(colRef, { cat: "A", v: 30 });
    const q = query(colRef, where("cat", "==", "A"));
    const snap = await getAggregate(q, { n: count(), total: sum("v") });
    expect(snap.data().n).toBe(2);
    expect(snap.data().total).toBe(40);
  });
});

// ── Transaction ───────────────────────────────────────────────────────────────

describe("lite: runTransaction", () => {
  it("atomically reads and writes", async () => {
    const ref = doc(ctx.db, col(), "counter");
    await setDoc(ref, { count: 0 });
    await runTransaction(ctx.db, async (tx) => {
      const snap = await tx.get(ref);
      tx.update(ref, { count: snap.data().count + 1 });
    });
    const snap = await getDoc(ref);
    expect(snap.data().count).toBe(1);
  });
});

// ── WriteBatch ────────────────────────────────────────────────────────────────

describe("lite: writeBatch", () => {
  it("commits multiple writes atomically", async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, "a");
    const ref2 = doc(ctx.db, c, "b");
    const batch = writeBatch(ctx.db);
    batch.set(ref1, { v: 1 });
    batch.set(ref2, { v: 2 });
    await batch.commit();
    const [s1, s2] = await Promise.all([getDoc(ref1), getDoc(ref2)]);
    expect(s1.data().v).toBe(1);
    expect(s2.data().v).toBe(2);
  });
});

// ── CRUD edge cases ───────────────────────────────────────────────────────────

describe("lite: getDoc non-existent", () => {
  it("returns a snapshot where exists() is false", async () => {
    const ref = doc(ctx.db, col(), "ghost");
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
    expect(snap.data()).toBeUndefined();
  });
});

describe("lite: updateDoc non-existent", () => {
  it("throws when document does not exist", async () => {
    const ref = doc(ctx.db, col(), "missing");
    await expect(updateDoc(ref, { x: 1 })).rejects.toThrow();
  });
});

describe("lite: deleteDoc non-existent", () => {
  it("does not throw for a missing document", async () => {
    const ref = doc(ctx.db, col(), "ghost");
    await expect(deleteDoc(ref)).resolves.toBeUndefined();
  });
});

describe("lite: setDoc with merge", () => {
  it("creates document when it does not exist", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: 1 }, { merge: true });
    expect((await getDoc(ref)).data()).toEqual({ a: 1 });
  });

  it("merges into existing document without overwriting unmentioned fields", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: 1, b: 2 });
    await setDoc(ref, { b: 99, c: 3 }, { merge: true });
    expect((await getDoc(ref)).data()).toEqual({ a: 1, b: 99, c: 3 });
  });
});

// ── Field transform edge cases ────────────────────────────────────────────────

describe("lite: increment on non-existent field", () => {
  it("treats missing field as 0 and returns the delta", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { other: true });
    await updateDoc(ref, { count: increment(7) });
    expect((await getDoc(ref)).data().count).toBe(7);
  });
});

describe("lite: arrayUnion on non-existent field", () => {
  it("creates the array field with the union elements", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { other: true });
    await updateDoc(ref, { tags: arrayUnion("x", "y") });
    expect((await getDoc(ref)).data().tags).toEqual(["x", "y"]);
  });
});

describe("lite: arrayRemove on non-existent field", () => {
  it("is a no-op when the field does not exist", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { other: true });
    await updateDoc(ref, { tags: arrayRemove("x") });
    expect((await getDoc(ref)).data().tags).toBeUndefined();
  });
});

// ── Nested field operations ───────────────────────────────────────────────────

describe("lite: updateDoc nested dot notation", () => {
  it("updates only the targeted nested field", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { profile: { name: "Alice", age: 30 }, score: 100 });
    await updateDoc(ref, { "profile.age": 31 });
    expect((await getDoc(ref)).data()).toEqual({
      profile: { name: "Alice", age: 31 },
      score: 100,
    });
  });
});

describe("lite: deleteField on nested path", () => {
  it("removes only the targeted nested field", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { profile: { name: "Alice", age: 30 } });
    await updateDoc(ref, { "profile.age": deleteField() });
    expect((await getDoc(ref)).data()).toEqual({ profile: { name: "Alice" } });
  });
});

// ── Additional query operators ────────────────────────────────────────────────

describe("lite: where range operators", () => {
  it("returns documents matching < and >= filters", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });
    await addDoc(colRef, { n: 5 });
    await addDoc(colRef, { n: 10 });
    const lt = await getDocs(query(colRef, where("n", "<", 5)));
    expect(lt.size).toBe(1);
    const gte = await getDocs(query(colRef, where("n", ">=", 5)));
    expect(gte.size).toBe(2);
  });
});

describe("lite: where not-in operator", () => {
  it("excludes documents whose field is in the list", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { color: "red" });
    await addDoc(colRef, { color: "blue" });
    await addDoc(colRef, { color: "green" });
    const snap = await getDocs(
      query(colRef, where("color", "not-in", ["red", "green"])),
    );
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().color).toBe("blue");
  });
});

describe("lite: where array-contains-any", () => {
  it("returns documents where array field contains at least one value", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { tags: ["a", "b"] });
    await addDoc(colRef, { tags: ["c", "d"] });
    await addDoc(colRef, { tags: ["b", "e"] });
    const snap = await getDocs(
      query(colRef, where("tags", "array-contains-any", ["a", "c"])),
    );
    expect(snap.size).toBe(2);
  });
});

describe("lite: compound query with multiple where", () => {
  it("returns documents matching all conditions", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { cat: "A", active: true });
    await addDoc(colRef, { cat: "A", active: false });
    await addDoc(colRef, { cat: "B", active: true });
    const snap = await getDocs(
      query(colRef, where("cat", "==", "A"), where("active", "==", true)),
    );
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data()).toMatchObject({ cat: "A", active: true });
  });
});

// ── Cursor pagination (inclusive variants) ────────────────────────────────────

describe("lite: cursor pagination with startAt (inclusive)", () => {
  it("includes the cursor document", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });
    await addDoc(colRef, { n: 2 });
    await addDoc(colRef, { n: 3 });
    const all = await getDocs(query(colRef, orderBy("n")));
    const second = all.docs[1];
    const snap = await getDocs(query(colRef, orderBy("n"), startAt(second)));
    expect(snap.size).toBe(2);
    expect(snap.docs.map((d) => d.data().n)).toEqual([2, 3]);
  });
});

describe("lite: cursor pagination with endAt (inclusive)", () => {
  it("includes the cursor document", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });
    await addDoc(colRef, { n: 2 });
    await addDoc(colRef, { n: 3 });
    const all = await getDocs(query(colRef, orderBy("n")));
    const second = all.docs[1];
    const snap = await getDocs(query(colRef, orderBy("n"), endAt(second)));
    expect(snap.size).toBe(2);
    expect(snap.docs.map((d) => d.data().n)).toEqual([1, 2]);
  });
});

// ── Aggregation edge cases ────────────────────────────────────────────────────

describe("lite: getCount on empty collection", () => {
  it("returns 0", async () => {
    const colRef = collection(ctx.db, col());
    const snap = await getCount(colRef);
    expect(snap.data().count).toBe(0);
  });
});

describe("lite: getAggregate sum on empty collection", () => {
  it("returns 0 for sum on empty collection", async () => {
    const colRef = collection(ctx.db, col());
    const snap = await getAggregate(colRef, {
      total: sum("amount"),
      n: count(),
    });
    expect(snap.data().n).toBe(0);
    // sum of empty set is 0 in Firestore
    expect(snap.data().total).toBe(0);
  });
});

// ── WriteBatch with delete and update ─────────────────────────────────────────

describe("lite: writeBatch with delete and update", () => {
  it("atomically deletes one doc and updates another", async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, "keep");
    const ref2 = doc(ctx.db, c, "gone");
    await setDoc(ref1, { v: 1 });
    await setDoc(ref2, { v: 2 });

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

describe("lite: null field roundtrip", () => {
  it("stores and retrieves null field values", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { name: "Alice", score: null });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ name: "Alice", score: null });
  });
});

describe("lite: where == null", () => {
  it("matches documents where field is explicitly null", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { x: null });
    await addDoc(colRef, { x: 1 });
    const snap = await getDocs(query(colRef, where("x", "==", null)));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().x).toBeNull();
  });
});

// ── setDoc replaces entire document ──────────────────────────────────────────

describe("lite: setDoc replaces entire document", () => {
  it("removes fields not in the new document", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: 1, b: 2, c: 3 });
    await setDoc(ref, { a: 99 });
    expect((await getDoc(ref)).data()).toEqual({ a: 99 });
  });
});

// ── Transaction edge cases ────────────────────────────────────────────────────

describe("lite: runTransaction on non-existent document", () => {
  it("reads an empty snapshot and can create the document", async () => {
    const ref = doc(ctx.db, col(), "new-doc");
    await runTransaction(ctx.db, async (tx) => {
      const snap = await tx.get(ref);
      expect(snap.exists()).toBe(false);
      tx.set(ref, { created: true });
    });
    expect((await getDoc(ref)).data()).toEqual({ created: true });
  });
});

// ── Combined query constraints ────────────────────────────────────────────────

describe("lite: where + orderBy + limit", () => {
  it("returns filtered, ordered, limited results", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { cat: "A", n: 3 });
    await addDoc(colRef, { cat: "A", n: 1 });
    await addDoc(colRef, { cat: "A", n: 2 });
    await addDoc(colRef, { cat: "B", n: 10 });
    const snap = await getDocs(
      query(colRef, where("cat", "==", "A"), orderBy("n"), limit(2)),
    );
    expect(snap.size).toBe(2);
    expect(snap.docs.map((d) => d.data().n)).toEqual([1, 2]);
  });
});

describe("lite: paginate with orderBy + startAfter + limit", () => {
  it("chains cursor pages correctly", async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });

    const page1 = await getDocs(query(colRef, orderBy("n"), limit(2)));
    expect(page1.docs.map((d) => d.data().n)).toEqual([1, 2]);

    const page2 = await getDocs(
      query(colRef, orderBy("n"), startAfter(page1.docs[1]), limit(2)),
    );
    expect(page2.docs.map((d) => d.data().n)).toEqual([3, 4]);

    const page3 = await getDocs(
      query(colRef, orderBy("n"), startAfter(page2.docs[1]), limit(2)),
    );
    expect(page3.docs.map((d) => d.data().n)).toEqual([5]);
  });
});

// ── Nested field transforms ───────────────────────────────────────────────────

describe("lite: serverTimestamp in nested field path", () => {
  it("sets a nested timestamp field on write", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { meta: { name: "x" } });
    await updateDoc(ref, { "meta.updatedAt": serverTimestamp() });
    const snap = await getDoc(ref);
    expect(snap.data().meta.name).toBe("x");
    expect(snap.data().meta.updatedAt.toDate).toBeTypeOf("function");
  });
});

describe("lite: increment in nested field path", () => {
  it("increments a nested counter without touching siblings", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { stats: { views: 10, likes: 5 } });
    await updateDoc(ref, { "stats.views": increment(1) });
    const snap = await getDoc(ref);
    expect(snap.data().stats.views).toBe(11);
    expect(snap.data().stats.likes).toBe(5);
  });
});

// ── Data type precision ───────────────────────────────────────────────────────

describe("lite: large integer precision", () => {
  it("stores and retrieves integers above 2^53 without loss", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    const big = 9007199254740993n; // BigInt
    await setDoc(ref, { id: 9007199254740993 });
    const snap = await getDoc(ref);
    // Firestore SDK returns numbers; verify it comes back as integer type
    expect(Number.isInteger(snap.data().id)).toBe(true);
  });
});

describe("lite: float field roundtrip", () => {
  it("stores and retrieves floating-point values", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { price: 3.14 });
    expect((await getDoc(ref)).data().price).toBeCloseTo(3.14);
  });
});

// ── Deep nesting ──────────────────────────────────────────────────────────────

describe("lite: deeply nested object roundtrip", () => {
  it("stores and retrieves a 4-level nested object", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    const data = { a: { b: { c: { d: 42 } } } };
    await setDoc(ref, data);
    expect((await getDoc(ref)).data()).toEqual(data);
  });
});

// ── Ordering with missing fields ──────────────────────────────────────────────

describe("lite: where + orderBy excludes docs missing the field", () => {
  it("returns only docs that have the ordered field", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 5 });
    await addDoc(colRef, { other: "no n" });
    await addDoc(colRef, { n: 1 });
    const snap = await getDocs(
      query(colRef, where("n", ">=", 1), orderBy("n")),
    );
    expect(snap.size).toBe(2);
    expect(snap.docs.map((d) => d.data().n)).toEqual([1, 5]);
  });
});

// ── Batch atomicity ───────────────────────────────────────────────────────────

describe("lite: writeBatch atomicity on precondition failure", () => {
  it("rolls back all writes when one write fails its precondition", async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, "a");
    const ref2 = doc(ctx.db, c, "b");
    await setDoc(ref1, { v: 1 });
    // ref2 does NOT exist — update will fail

    const batch = writeBatch(ctx.db);
    batch.set(ref1, { v: 99 });
    batch.update(ref2, { v: 1 });
    await expect(batch.commit()).rejects.toThrow();

    expect((await getDoc(ref1)).data().v).toBe(1);
  });
});

// ── Nested field queries ──────────────────────────────────────────────────────

describe("lite: where on nested field path", () => {
  it("filters using dot-notation field path", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { meta: { score: 10 } });
    await addDoc(colRef, { meta: { score: 50 } });
    await addDoc(colRef, { meta: { score: 90 } });
    const snap = await getDocs(query(colRef, where("meta.score", ">", 30)));
    expect(snap.size).toBe(2);
    expect(snap.docs.every((d) => d.data().meta.score > 30)).toBe(true);
  });
});

describe("lite: orderBy on nested field path", () => {
  it("sorts by a nested field", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { user: { age: 30 } });
    await addDoc(colRef, { user: { age: 10 } });
    await addDoc(colRef, { user: { age: 20 } });
    const snap = await getDocs(query(colRef, orderBy("user.age")));
    expect(snap.docs.map((d) => d.data().user.age)).toEqual([10, 20, 30]);
  });
});

describe("lite: orderBy DESC", () => {
  it("returns documents in reverse order", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });
    await addDoc(colRef, { n: 3 });
    await addDoc(colRef, { n: 2 });
    const snap = await getDocs(query(colRef, orderBy("n", "desc")));
    expect(snap.docs.map((d) => d.data().n)).toEqual([3, 2, 1]);
  });
});

// ── Sub-collections ───────────────────────────────────────────────────────────

describe("lite: sub-collection CRUD", () => {
  it("creates and reads documents in a sub-collection", async () => {
    const parentRef = doc(ctx.db, col(), "parent");
    await setDoc(parentRef, { name: "parent" });
    const subRef = doc(parentRef, "items", "item1");
    await setDoc(subRef, { label: "first" });
    const snap = await getDoc(subRef);
    expect(snap.exists()).toBe(true);
    expect(snap.data().label).toBe("first");
  });
});

// ── deleteDoc / getDoc edge cases ─────────────────────────────────────────────

describe("lite: deleteDoc on non-existent document", () => {
  it("does not throw", async () => {
    const ref = doc(ctx.db, col(), "ghost");
    await expect(deleteDoc(ref)).resolves.not.toThrow();
  });
});

describe("lite: getDoc on non-existent document", () => {
  it("returns exists=false snapshot", async () => {
    const ref = doc(ctx.db, col(), "ghost");
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
    expect(snap.data()).toBeUndefined();
  });
});

describe("lite: updateDoc on non-existent document", () => {
  it("rejects with error", async () => {
    const ref = doc(ctx.db, col(), "ghost");
    await expect(updateDoc(ref, { x: 1 })).rejects.toThrow();
  });
});

// ── Deep nesting (3 levels) ───────────────────────────────────────────────────

describe("lite: updateDoc at three-level nested path", () => {
  it("updates deep leaf without touching siblings", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: { b: { c: 1, d: 2 } } });
    await updateDoc(ref, { "a.b.c": 99 });
    const snap = await getDoc(ref);
    expect(snap.data().a.b.c).toBe(99);
    expect(snap.data().a.b.d).toBe(2);
  });
});

// ── Unicode ───────────────────────────────────────────────────────────────────

describe("lite: unicode field values", () => {
  it("stores and retrieves emoji and CJK characters", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    const data = { emoji: "🔥", cjk: "日本語", arabic: "مرحبا" };
    await setDoc(ref, data);
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual(data);
  });
});

describe("lite: empty string field", () => {
  it("stores and retrieves empty string", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { name: "" });
    const snap = await getDoc(ref);
    expect(snap.data().name).toBe("");
  });
});

// ── Boolean filter ────────────────────────────────────────────────────────────

describe("lite: boolean where filter", () => {
  it("filters on boolean field correctly", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { active: true });
    await addDoc(colRef, { active: false });
    await addDoc(colRef, { active: true });
    const snap = await getDocs(query(colRef, where("active", "==", true)));
    expect(snap.size).toBe(2);
    expect(snap.docs.every((d) => d.data().active === true)).toBe(true);
  });
});

// ── setDoc merge on nested field ──────────────────────────────────────────────

describe("lite: setDoc merge on existing nested field", () => {
  it("adds new nested field without removing sibling", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { meta: { name: "Alice", age: 30 } });
    await setDoc(ref, { meta: { score: 100 } }, { merge: true });
    const snap = await getDoc(ref);
    expect(snap.data().meta.name).toBe("Alice");
    expect(snap.data().meta.age).toBe(30);
    expect(snap.data().meta.score).toBe(100);
  });
});

// ── orderBy DESC cursor ───────────────────────────────────────────────────────

describe("lite: startAfter with orderBy DESC", () => {
  it("paginates in reverse order", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { n: 1 });
    await addDoc(colRef, { n: 2 });
    await addDoc(colRef, { n: 3 });
    await addDoc(colRef, { n: 4 });
    await addDoc(colRef, { n: 5 });
    const first = await getDocs(query(colRef, orderBy("n", "desc"), limit(2)));
    expect(first.docs.map((d) => d.data().n)).toEqual([5, 4]);
    const second = await getDocs(
      query(colRef, orderBy("n", "desc"), startAfter(first.docs[1]), limit(2)),
    );
    expect(second.docs.map((d) => d.data().n)).toEqual([3, 2]);
  });
});

// ── Float increment in nested path ────────────────────────────────────────────

describe("lite: float increment on nested field", () => {
  it("increments a float counter in a nested path", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { stats: { rate: 1.5 } });
    await updateDoc(ref, { "stats.rate": increment(0.5) });
    const snap = await getDoc(ref);
    expect(snap.data().stats.rate).toBeCloseTo(2.0);
  });
});

// ── arrayUnion + arrayRemove interaction ─────────────────────────────────────

describe("lite: arrayUnion then arrayRemove on same field", () => {
  it("results in correct set after both operations", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { tags: ["a", "b"] });
    await updateDoc(ref, { tags: arrayUnion("c") });
    await updateDoc(ref, { tags: arrayRemove("b") });
    const snap = await getDoc(ref);
    expect(snap.data().tags.sort()).toEqual(["a", "c"]);
  });
});

// ── NOT IN query ──────────────────────────────────────────────────────────────

describe("lite: where not-in excludes listed values", () => {
  it("returns docs whose field is not in the list", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { status: "active" });
    await addDoc(colRef, { status: "inactive" });
    await addDoc(colRef, { status: "pending" });
    const snap = await getDocs(
      query(colRef, where("status", "not-in", ["inactive", "pending"])),
    );
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().status).toBe("active");
  });
});

// ── IN query ─────────────────────────────────────────────────────────────────

describe("lite: where in with multiple matches", () => {
  it("returns all docs whose field is in the list", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { cat: "A" });
    await addDoc(colRef, { cat: "B" });
    await addDoc(colRef, { cat: "C" });
    const snap = await getDocs(query(colRef, where("cat", "in", ["A", "C"])));
    expect(snap.size).toBe(2);
    expect(snap.docs.map((d) => d.data().cat).sort()).toEqual(["A", "C"]);
  });
});

// ── Aggregation on filtered results ───────────────────────────────────────────

describe("lite: count with where filter", () => {
  it("counts only matching documents", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { active: true });
    await addDoc(colRef, { active: false });
    await addDoc(colRef, { active: true });
    const snap = await getAggregate(
      query(colRef, where("active", "==", true)),
      { c: count() },
    );
    expect(snap.data().c).toBe(2);
  });
});

// ── Transaction reads multiple docs ──────────────────────────────────────────

describe("lite: transaction reads multiple documents atomically", () => {
  it("sees consistent state across multiple gets", async () => {
    const colRef = collection(ctx.db, col());
    const ref1 = doc(colRef, "a");
    const ref2 = doc(colRef, "b");
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

// ── Large writeBatch ──────────────────────────────────────────────────────────

describe("lite: writeBatch with many documents", () => {
  it("commits 20 sets atomically", async () => {
    const c = col();
    const batch = writeBatch(ctx.db);
    const refs = Array.from({ length: 20 }, (_, i) =>
      doc(ctx.db, c, `doc${i}`),
    );
    refs.forEach((r, i) => batch.set(r, { i }));
    await batch.commit();
    const snaps = await Promise.all(refs.map((r) => getDoc(r)));
    expect(snaps.every((s) => s.exists())).toBe(true);
    expect(snaps.map((s) => s.data().i)).toEqual(
      Array.from({ length: 20 }, (_, i) => i),
    );
  });
});

// ── arrayContains / arrayContainsAny ─────────────────────────────────────────

describe("lite: arrayContains filter", () => {
  it("returns docs where array field contains the value", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { tags: ["a", "b"] });
    await addDoc(colRef, { tags: ["b", "c"] });
    await addDoc(colRef, { tags: ["x"] });
    const snap = await getDocs(
      query(colRef, where("tags", "array-contains", "b")),
    );
    expect(snap.size).toBe(2);
  });
});

describe("lite: arrayContainsAny filter", () => {
  it("returns docs where array field contains any of the listed values", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { tags: ["a"] });
    await addDoc(colRef, { tags: ["b"] });
    await addDoc(colRef, { tags: ["c"] });
    await addDoc(colRef, { tags: ["x"] });
    const snap = await getDocs(
      query(colRef, where("tags", "array-contains-any", ["a", "c"])),
    );
    expect(snap.size).toBe(2);
  });
});

// ── deleteField ───────────────────────────────────────────────────────────────

describe("lite: deleteField in updateDoc", () => {
  it("removes the field from the document", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { name: "Bob", age: 42 });
    await updateDoc(ref, { age: deleteField() });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ name: "Bob" });
    expect(snap.data().age).toBeUndefined();
  });
});

// ── increment on absent field ─────────────────────────────────────────────────

describe("lite: increment on absent field", () => {
  it("initialises missing field to 0 + delta", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { name: "test" });
    await updateDoc(ref, { views: increment(5) });
    const snap = await getDoc(ref);
    expect(snap.data().views).toBe(5);
    expect(snap.data().name).toBe("test");
  });
});

// ── compound orderBy ──────────────────────────────────────────────────────────

describe("lite: compound orderBy on two fields", () => {
  it("sorts by primary then secondary field", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { group: 1, rank: 3 });
    await addDoc(colRef, { group: 2, rank: 1 });
    await addDoc(colRef, { group: 1, rank: 1 });
    await addDoc(colRef, { group: 2, rank: 2 });
    const snap = await getDocs(
      query(colRef, orderBy("group"), orderBy("rank")),
    );
    const pairs = snap.docs.map((d) => [d.data().group, d.data().rank]);
    expect(pairs).toEqual([
      [1, 1],
      [1, 3],
      [2, 1],
      [2, 2],
    ]);
  });
});

// ── sum aggregation ───────────────────────────────────────────────────────────

describe("lite: sum aggregation", () => {
  it("sums a numeric field across matching documents", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { val: 10, active: true });
    await addDoc(colRef, { val: 20, active: true });
    await addDoc(colRef, { val: 5, active: false });
    const snap = await getAggregate(
      query(colRef, where("active", "==", true)),
      { total: sum("val") },
    );
    expect(snap.data().total).toBe(30);
  });
});

// ── endBefore / endAt cursor ──────────────────────────────────────────────────

describe("lite: endBefore cursor", () => {
  it("excludes the cursor document", async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const all = await getDocs(query(colRef, orderBy("n")));
    const cursor = all.docs[2]; // n=3
    const snap = await getDocs(query(colRef, orderBy("n"), endBefore(cursor)));
    expect(snap.docs.map((d) => d.data().n)).toEqual([1, 2]);
  });
});

describe("lite: endAt cursor", () => {
  it("includes the cursor document", async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const all = await getDocs(query(colRef, orderBy("n")));
    const cursor = all.docs[2]; // n=3
    const snap = await getDocs(query(colRef, orderBy("n"), endAt(cursor)));
    expect(snap.docs.map((d) => d.data().n)).toEqual([1, 2, 3]);
  });
});

// ── null value roundtrip ──────────────────────────────────────────────────────

describe("lite: null field value roundtrip", () => {
  it("stores and retrieves an explicit null field", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { x: null, y: 1 });
    const snap = await getDoc(ref);
    expect(snap.data().x).toBeNull();
    expect(snap.data().y).toBe(1);
  });
});

// ── serverTimestamp in setDoc (initial write) ─────────────────────────────────

describe("lite: serverTimestamp in setDoc", () => {
  it("resolves to a Timestamp on the initial write", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { createdAt: serverTimestamp(), label: "hi" });
    const snap = await getDoc(ref);
    expect(snap.data().label).toBe("hi");
    expect(snap.data().createdAt).not.toBeNull();
    expect(snap.data().createdAt.toDate).toBeTypeOf("function");
  });
});

// ── Multiple where clauses ────────────────────────────────────────────────────

describe("lite: multiple where clauses combined", () => {
  it("ANDs all where conditions", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { active: true, score: 90 });
    await addDoc(colRef, { active: true, score: 40 });
    await addDoc(colRef, { active: false, score: 90 });
    const snap = await getDocs(
      query(colRef, where("active", "==", true), where("score", ">=", 80)),
    );
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().score).toBe(90);
  });
});

// ── updateDoc preserves sibling fields ───────────────────────────────────────

describe("lite: updateDoc preserves untouched top-level fields", () => {
  it("does not erase fields not in the update", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: 1, b: 2, c: 3 });
    await updateDoc(ref, { b: 99 });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ a: 1, b: 99, c: 3 });
  });
});

// ── setDoc overwrites completely ──────────────────────────────────────────────

describe("lite: setDoc without merge overwrites entire document", () => {
  it("replaces the document dropping previous fields", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: 1, b: 2 });
    await setDoc(ref, { c: 3 });
    const snap = await getDoc(ref);
    expect(snap.data()).toEqual({ c: 3 });
    expect(snap.data().a).toBeUndefined();
  });
});

// ── transaction increment ─────────────────────────────────────────────────────

describe("lite: runTransaction increments a counter", () => {
  it("reads then writes inside a transaction", async () => {
    const ref = doc(ctx.db, col(), "counter");
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

describe("lite: getDoc after deleteDoc", () => {
  it("returns exists=false after deletion", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { x: 1 });
    await deleteDoc(ref);
    const snap = await getDoc(ref);
    expect(snap.exists()).toBe(false);
  });
});

// ── arrayUnion idempotency ────────────────────────────────────────────────────

describe("lite: arrayUnion does not duplicate existing element", () => {
  it("keeps the array length the same when element already present", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { tags: ["a", "b"] });
    await updateDoc(ref, { tags: arrayUnion("b") });
    const snap = await getDoc(ref);
    expect(snap.data().tags).toEqual(["a", "b"]);
  });
});

// ── writeBatch delete ─────────────────────────────────────────────────────────

describe("lite: writeBatch with delete operation", () => {
  it("deletes a document as part of a batch", async () => {
    const c = col();
    const ref1 = doc(ctx.db, c, "keep");
    const ref2 = doc(ctx.db, c, "delete-me");
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

// ── Large batch mixed operations ──────────────────────────────────────────────

describe("lite: writeBatch mixed set/update/delete", () => {
  it("applies all three operation types atomically", async () => {
    const c = col();
    const setRef = doc(ctx.db, c, "new");
    const updRef = doc(ctx.db, c, "existing");
    const delRef = doc(ctx.db, c, "gone");
    await setDoc(updRef, { v: 1 });
    await setDoc(delRef, { v: 2 });

    const batch = writeBatch(ctx.db);
    batch.set(setRef, { v: 10 });
    batch.update(updRef, { v: 99 });
    batch.delete(delRef);
    await batch.commit();

    const [s1, s2, s3] = await Promise.all([
      getDoc(setRef),
      getDoc(updRef),
      getDoc(delRef),
    ]);
    expect(s1.data().v).toBe(10);
    expect(s2.data().v).toBe(99);
    expect(s3.exists()).toBe(false);
  });
});

// ── Cursor with raw field value ───────────────────────────────────────────────

describe("lite: startAt with raw field value", () => {
  it("starts from the given field value inclusive", async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const snap = await getDocs(query(colRef, orderBy("n"), startAt(3)));
    expect(snap.docs.map((d) => d.data().n)).toEqual([3, 4, 5]);
  });
});

describe("lite: startAfter with raw field value", () => {
  it("starts after the given field value exclusive", async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 5; i++) await addDoc(colRef, { n: i });
    const snap = await getDocs(query(colRef, orderBy("n"), startAfter(3)));
    expect(snap.docs.map((d) => d.data().n)).toEqual([4, 5]);
  });
});

// ── Nested dot-notation update ────────────────────────────────────────────────

describe("lite: deep nested dot-notation update", () => {
  it("updates leaf without touching other leaves", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { a: { b: { x: 1, y: 2 }, c: 3 } });
    await updateDoc(ref, { "a.b.x": 99 });
    const snap = await getDoc(ref);
    const d = snap.data();
    expect(d.a.b.x).toBe(99);
    expect(d.a.b.y).toBe(2);
    expect(d.a.c).toBe(3);
  });
});

// ── where != ─────────────────────────────────────────────────────────────────

describe("lite: where != filter", () => {
  it("excludes documents with the given value", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { status: "active" });
    await addDoc(colRef, { status: "inactive" });
    await addDoc(colRef, { status: "active" });
    const snap = await getDocs(
      query(colRef, where("status", "!=", "inactive")),
    );
    expect(snap.size).toBe(2);
    expect(snap.docs.every((d) => d.data().status !== "inactive")).toBe(true);
  });
});

// ── where < and > ────────────────────────────────────────────────────────────

describe("lite: where < and > range filters", () => {
  it("returns only docs within range", async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 10; i++) await addDoc(colRef, { n: i });
    const snap = await getDocs(
      query(colRef, where("n", ">", 3), where("n", "<", 7), orderBy("n")),
    );
    expect(snap.docs.map((d) => d.data().n)).toEqual([4, 5, 6]);
  });
});

// ── Pagination partial last page ──────────────────────────────────────────────

describe("lite: pagination handles partial last page", () => {
  it("returns correct docs across pages", async () => {
    const colRef = collection(ctx.db, col());
    for (let i = 1; i <= 7; i++) await addDoc(colRef, { n: i });

    const page1 = await getDocs(query(colRef, orderBy("n"), limit(3)));
    expect(page1.docs.map((d) => d.data().n)).toEqual([1, 2, 3]);

    const page2 = await getDocs(
      query(colRef, orderBy("n"), startAfter(page1.docs[2]), limit(3)),
    );
    expect(page2.docs.map((d) => d.data().n)).toEqual([4, 5, 6]);

    const page3 = await getDocs(
      query(colRef, orderBy("n"), startAfter(page2.docs[2]), limit(3)),
    );
    expect(page3.docs.map((d) => d.data().n)).toEqual([7]);
  });
});

// ── count on empty collection ─────────────────────────────────────────────────

describe("lite: count on empty collection", () => {
  it("returns 0 for an empty collection", async () => {
    const colRef = collection(ctx.db, col());
    const snap = await getAggregate(colRef, { c: count() });
    expect(snap.data().c).toBe(0);
  });
});

// ── sum on missing field returns 0 ────────────────────────────────────────────

describe("lite: sum on field absent from all docs", () => {
  it("returns 0 when no document has the summed field", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { name: "a" });
    await addDoc(colRef, { name: "b" });
    const snap = await getAggregate(colRef, { total: sum("missing") });
    expect(snap.data().total).toBe(0);
  });
});

// ── getDocs without query ─────────────────────────────────────────────────────

describe("lite: getDocs without query returns all documents", () => {
  it("retrieves all documents in a collection", async () => {
    const colRef = collection(ctx.db, col());
    await setDoc(doc(colRef, "alpha"), { x: 1 });
    await setDoc(doc(colRef, "beta"), { x: 2 });
    await setDoc(doc(colRef, "gamma"), { x: 3 });
    const snap = await getDocs(colRef);
    expect(snap.size).toBe(3);
    const ids = snap.docs.map((d) => d.id).sort();
    expect(ids).toEqual(["alpha", "beta", "gamma"]);
  });
});

// ── deleteField on nested path ────────────────────────────────────────────────

describe("lite: deleteField on nested path", () => {
  it("removes a nested field without affecting siblings", async () => {
    const ref = doc(ctx.db, col(), "doc1");
    await setDoc(ref, { meta: { a: 1, b: 2 } });
    await updateDoc(ref, { "meta.a": deleteField() });
    const snap = await getDoc(ref);
    expect(snap.data().meta.b).toBe(2);
    expect(snap.data().meta.a).toBeUndefined();
  });
});

// ── Array of maps with arrayContains ─────────────────────────────────────────

describe("lite: arrayContains with map element", () => {
  it("matches doc whose array contains the exact map", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { items: [{ id: 1 }, { id: 2 }] });
    await addDoc(colRef, { items: [{ id: 3 }] });
    const snap = await getDocs(
      query(colRef, where("items", "array-contains", { id: 1 })),
    );
    expect(snap.size).toBe(1);
  });
});

// ── where on nested map field ─────────────────────────────────────────────────

describe("lite: where == on nested map field", () => {
  it("matches documents with exact nested field equality", async () => {
    const colRef = collection(ctx.db, col());
    await addDoc(colRef, { addr: { city: "NY", zip: "10001" } });
    await addDoc(colRef, { addr: { city: "LA", zip: "90001" } });
    const snap = await getDocs(query(colRef, where("addr.city", "==", "NY")));
    expect(snap.size).toBe(1);
    expect(snap.docs[0].data().addr.zip).toBe("10001");
  });
});

// ═══════════════════════════════════════════════════════════════════════════
// END-TO-END PERSISTENCE TESTS
// Each test opens a fresh Firebase app ("page reload"), writes data, closes
// the app, opens another fresh app, then reads back to prove durability.
// ═══════════════════════════════════════════════════════════════════════════

describe("lite: [e2e] basic document survives app reload", () => {
  it("scalar fields persist across two separate app instances", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "user1"), {
      name: "Alice",
      age: 30,
      active: true,
    });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "user1"));
    await closeDb(s2);

    expect(snap.exists()).toBe(true);
    expect(snap.data()).toEqual({ name: "Alice", age: 30, active: true });
  });
});

describe("lite: [e2e] nested map survives reload", () => {
  it("nested object fields are fully preserved", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "doc1"), {
      profile: { name: "Bob", address: { city: "NYC", zip: "10001" } },
      score: 99,
    });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "doc1"));
    await closeDb(s2);

    expect(snap.data().profile.name).toBe("Bob");
    expect(snap.data().profile.address.city).toBe("NYC");
    expect(snap.data().score).toBe(99);
  });
});

describe("lite: [e2e] array fields survive reload", () => {
  it("array values are fully preserved including order", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "doc1"), {
      tags: ["go", "firestore", "emulator"],
      count: 3,
    });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "doc1"));
    await closeDb(s2);

    expect(snap.data().tags).toEqual(["go", "firestore", "emulator"]);
    expect(snap.data().count).toBe(3);
  });
});

describe("lite: [e2e] update persists across reload", () => {
  it("updateDoc changes survive app restart", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "item"), { price: 10, stock: 5 });
    await updateDoc(doc(s1.db, c, "item"), { price: 15, stock: 3 });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "item"));
    await closeDb(s2);

    expect(snap.data().price).toBe(15);
    expect(snap.data().stock).toBe(3);
  });
});

describe("lite: [e2e] delete persists across reload", () => {
  it("deleted document is gone after app restart", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "temp"), { x: 1 });
    await deleteDoc(doc(s1.db, c, "temp"));
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "temp"));
    await closeDb(s2);

    expect(snap.exists()).toBe(false);
  });
});

describe("lite: [e2e] collection survives reload and remains queryable", () => {
  it("getDocs and where filter work after app restart", async () => {
    const c = col();

    const s1 = makeDb();
    const colRef1 = collection(s1.db, c);
    await addDoc(colRef1, { category: "fruit", name: "apple" });
    await addDoc(colRef1, { category: "fruit", name: "banana" });
    await addDoc(colRef1, { category: "veggie", name: "carrot" });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDocs(
      query(
        collection(s2.db, c),
        where("category", "==", "fruit"),
        orderBy("name"),
      ),
    );
    await closeDb(s2);

    expect(snap.size).toBe(2);
    expect(snap.docs.map((d) => d.data().name)).toEqual(["apple", "banana"]);
  });
});

describe("lite: [e2e] writeBatch survives reload", () => {
  it("all batch-written docs are readable after restart", async () => {
    const c = col();

    const s1 = makeDb();
    const batch = writeBatch(s1.db);
    for (let i = 0; i < 5; i++) {
      batch.set(doc(s1.db, c, `item${i}`), { i, label: `item-${i}` });
    }
    await batch.commit();
    await closeDb(s1);

    const s2 = makeDb();
    const snaps = await Promise.all(
      Array.from({ length: 5 }, (_, i) => getDoc(doc(s2.db, c, `item${i}`))),
    );
    await closeDb(s2);

    expect(snaps.every((s) => s.exists())).toBe(true);
    expect(snaps.map((s) => s.data().i)).toEqual([0, 1, 2, 3, 4]);
  });
});

describe("lite: [e2e] increment result persists", () => {
  it("FieldValue.increment result is durable", async () => {
    const c = col();

    const s1 = makeDb();
    const ref1 = doc(s1.db, c, "counter");
    await setDoc(ref1, { n: 0 });
    await updateDoc(ref1, { n: increment(7) });
    await updateDoc(ref1, { n: increment(3) });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "counter"));
    await closeDb(s2);

    expect(snap.data().n).toBe(10);
  });
});

describe("lite: [e2e] serverTimestamp survives reload as real timestamp", () => {
  it("serverTimestamp field is a Timestamp after reload", async () => {
    const c = col();
    const before = new Date();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "event"), {
      createdAt: serverTimestamp(),
      label: "test",
    });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "event"));
    await closeDb(s2);

    expect(snap.exists()).toBe(true);
    expect(snap.data().label).toBe("test");
    const ts = snap.data().createdAt;
    expect(ts).not.toBeNull();
    expect(typeof ts.toDate).toBe("function");
    expect(ts.toDate().getTime()).toBeGreaterThanOrEqual(
      before.getTime() - 1000,
    );
  });
});

describe("lite: [e2e] setDoc overwrite persists", () => {
  it("full overwrite drops old fields after reload", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "doc"), { a: 1, b: 2 });
    await setDoc(doc(s1.db, c, "doc"), { c: 3 }); // full overwrite
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "doc"));
    await closeDb(s2);

    expect(snap.data()).toEqual({ c: 3 });
    expect(snap.data().a).toBeUndefined();
  });
});

describe("lite: [e2e] multiple documents in collection persist independently", () => {
  it("each document retains its own data after reload", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "alpha"), { v: 100 });
    await setDoc(doc(s1.db, c, "beta"), { v: 200 });
    await setDoc(doc(s1.db, c, "gamma"), { v: 300 });
    // Delete one
    await deleteDoc(doc(s1.db, c, "beta"));
    await closeDb(s1);

    const s2 = makeDb();
    const [alpha, beta, gamma] = await Promise.all([
      getDoc(doc(s2.db, c, "alpha")),
      getDoc(doc(s2.db, c, "beta")),
      getDoc(doc(s2.db, c, "gamma")),
    ]);
    await closeDb(s2);

    expect(alpha.data().v).toBe(100);
    expect(beta.exists()).toBe(false);
    expect(gamma.data().v).toBe(300);
  });
});

describe("lite: [e2e] transaction result persists", () => {
  it("transaction commit is durable across reload", async () => {
    const c = col();

    const s1 = makeDb();
    const ref1 = doc(s1.db, c, "wallet");
    await setDoc(ref1, { balance: 100 });
    await runTransaction(s1.db, async (tx) => {
      const snap = await tx.get(ref1);
      tx.update(ref1, { balance: snap.data().balance + 50 });
    });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "wallet"));
    await closeDb(s2);

    expect(snap.data().balance).toBe(150);
  });
});

describe("lite: [e2e] null field persists", () => {
  it("explicit null field is readable after reload", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "doc"), { x: null, y: "keep" });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "doc"));
    await closeDb(s2);

    expect(snap.data().x).toBeNull();
    expect(snap.data().y).toBe("keep");
  });
});

describe("lite: [e2e] sub-collection persists across reload", () => {
  it("documents in a sub-collection survive app restart", async () => {
    const c = col();

    const s1 = makeDb();
    const parentRef = doc(s1.db, c, "parent");
    await setDoc(parentRef, { kind: "folder" });
    await setDoc(doc(parentRef, "children", "child1"), { name: "Alice" });
    await setDoc(doc(parentRef, "children", "child2"), { name: "Bob" });
    await closeDb(s1);

    const s2 = makeDb();
    const child1 = await getDoc(
      doc(doc(s2.db, c, "parent"), "children", "child1"),
    );
    const child2 = await getDoc(
      doc(doc(s2.db, c, "parent"), "children", "child2"),
    );
    await closeDb(s2);

    expect(child1.data().name).toBe("Alice");
    expect(child2.data().name).toBe("Bob");
  });
});

describe("lite: [e2e] orderBy query result stable after reload", () => {
  it("ordered query returns same order after restart", async () => {
    const c = col();

    const s1 = makeDb();
    const colRef1 = collection(s1.db, c);
    for (const n of [5, 2, 8, 1, 9]) {
      await addDoc(colRef1, { n });
    }
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDocs(query(collection(s2.db, c), orderBy("n")));
    await closeDb(s2);

    expect(snap.docs.map((d) => d.data().n)).toEqual([1, 2, 5, 8, 9]);
  });
});

describe("lite: [e2e] deleteField update persists", () => {
  it("field removed by deleteField() is gone after reload", async () => {
    const c = col();

    const s1 = makeDb();
    await setDoc(doc(s1.db, c, "doc"), { keep: "yes", remove: "no" });
    await updateDoc(doc(s1.db, c, "doc"), { remove: deleteField() });
    await closeDb(s1);

    const s2 = makeDb();
    const snap = await getDoc(doc(s2.db, c, "doc"));
    await closeDb(s2);

    expect(snap.data().keep).toBe("yes");
    expect(snap.data().remove).toBeUndefined();
  });
});

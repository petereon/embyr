import { test, expect } from '@playwright/test';

// Load the test harness page and wait for Firebase SDK to initialise.
async function openHarness(page) {
  // Capture browser console for debugging.
  page.on('console', msg => console.log(`[browser:${msg.type()}]`, msg.text()));
  page.on('pageerror', err => console.error('[browser:pageerror]', err.message));
  page.on('request',  req => { if (req.url().includes('17081') || req.url().includes('firestore')) console.log('[net:req]', req.method(), req.url().slice(0, 120)); });
  page.on('response', res => { if (res.url().includes('17081') || res.url().includes('firestore')) console.log('[net:res]', res.status(), res.url().slice(0, 120)); });

  await page.goto('/test-harness.html');
  await page.waitForFunction(() => window.__testHarnessReady === true, { timeout: 10_000 });
}

// ── helpers ──────────────────────────────────────────────────────────────────

/**
 * Seed docs and return the collection name. Uses a temporary db for writes only.
 * Each test creates its own fresh db to avoid SDK backoff from a prior unsub.
 */
async function seedDocs(page, cats) {
  return page.evaluate(async (cats) => {
    const { makeTestDb, closeTestDb, col, addDoc, collection } = window.__testHarness;
    const ctx = makeTestDb();
    const colName = col();
    const colRef = collection(ctx.db, colName);
    await Promise.all(cats.map((cat, i) => addDoc(colRef, { cat, v: i + 1 })));
    await closeTestDb(ctx);
    window.__testColName = colName;
    return colName;
  }, cats);
}

// ── tests ─────────────────────────────────────────────────────────────────────

test.describe('onSnapshot rapid filter switch — BrowserChannel transport', () => {
  // Each test gets a fresh page so Firebase SDK state is clean.
  test.beforeEach(async ({ page }) => { await openHarness(page); });

  test('second filter snapshot arrives within 2s when switched before first CURRENT', async ({ page }) => {
    await seedDocs(page, ['A', 'A', 'B']);

    const result = await page.evaluate(async () => {
      const { makeTestDb, collection, query, where, onSnapshot } = window.__testHarness;
      const { db } = makeTestDb();
      const colRef = collection(db, window.__testColName);

      const start = Date.now();
      let unsub2 = null;

      const result = await new Promise((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error(`query2 snapshot not received within 4s (${Date.now() - start}ms)`)),
          4000,
        );

        // Subscribe to query1 — do NOT await its snapshot before switching.
        const unsub1 = onSnapshot(query(colRef, where('cat', '==', 'A')), () => {}, reject);

        // Immediately subscribe to query2 while query1 is still pending CURRENT.
        unsub2 = onSnapshot(
          query(colRef, where('cat', '==', 'B')),
          snap => {
            clearTimeout(timeout);
            resolve({
              ms:   Date.now() - start,
              size: snap.size,
              allB: snap.docs.every(d => d.data().cat === 'B'),
            });
          },
          reject,
        );

        // Remove query1 AFTER query2 is registered (subscribe-before-unsubscribe).
        // query1 may still be pending CURRENT at this point.
        unsub1();
      });

      unsub2?.();
      return result;
    });

    expect(result.allB,  'all docs should have cat=B').toBe(true);
    expect(result.size,  'should have one B doc').toBe(1);
    expect(result.ms,    'snapshot must arrive in < 2s').toBeLessThan(2000);
  });

  test('third filter snapshot arrives within 2s after two rapid switches', async ({ page }) => {
    await seedDocs(page, ['A', 'B', 'C']);

    const result = await page.evaluate(async () => {
      const { makeTestDb, collection, query, where, onSnapshot } = window.__testHarness;
      const { db } = makeTestDb();
      const colRef = collection(db, window.__testColName);

      const start = Date.now();
      let unsub3 = null;

      const result = await new Promise((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error(`query3 snapshot not received within 4s (${Date.now() - start}ms)`)),
          4000,
        );

        const unsub1 = onSnapshot(query(colRef, where('cat', '==', 'A')), () => {}, reject);
        const unsub2 = onSnapshot(query(colRef, where('cat', '==', 'B')), () => {}, reject);

        unsub3 = onSnapshot(
          query(colRef, where('cat', '==', 'C')),
          snap => {
            clearTimeout(timeout);
            resolve({
              ms:   Date.now() - start,
              size: snap.size,
              allC: snap.docs.every(d => d.data().cat === 'C'),
            });
          },
          reject,
        );

        unsub1();
        unsub2();
      });

      unsub3?.();
      return result;
    });

    expect(result.allC,  'all docs should have cat=C').toBe(true);
    expect(result.size,  'should have one C doc').toBe(1);
    expect(result.ms,    'snapshot must arrive in < 2s').toBeLessThan(2000);
  });

  test('onSnapshot initial snapshot delivers correct documents', async ({ page }) => {
    await seedDocs(page, ['A', 'B', 'B']);

    const result = await page.evaluate(async () => {
      const { makeTestDb, collection, query, where, onSnapshot } = window.__testHarness;
      const { db } = makeTestDb();
      const colRef = collection(db, window.__testColName);

      return new Promise((resolve, reject) => {
        const unsub = onSnapshot(
          query(colRef, where('cat', '==', 'B')),
          snap => { unsub(); resolve({ size: snap.size, allB: snap.docs.every(d => d.data().cat === 'B') }); },
          reject,
        );
        setTimeout(() => reject(new Error('snapshot timeout')), 4000);
      });
    });

    expect(result.size).toBe(2);
    expect(result.allB).toBe(true);
  });

  test('live update delivered via BrowserChannel after addDoc', async ({ page }) => {
    await seedDocs(page, ['A']);

    const result = await page.evaluate(async () => {
      const { makeTestDb, collection, onSnapshot, addDoc } = window.__testHarness;
      const { db } = makeTestDb();
      const colRef = collection(db, window.__testColName);

      const snapshots = [];
      return new Promise((resolve, reject) => {
        const unsub = onSnapshot(colRef, snap => {
          snapshots.push(snap.size);
          if (snapshots.length === 2) {
            unsub();
            resolve(snapshots);
          }
        }, reject);

        setTimeout(() => addDoc(colRef, { cat: 'A', v: 99 }).catch(reject), 100);
        setTimeout(() => reject(new Error('live update timeout')), 4000);
      });
    });

    expect(result[0]).toBe(1); // initial snapshot
    expect(result[1]).toBe(2); // after addDoc
  });
});

import { test, expect } from '@playwright/test';

// Repro for the user-reported "first switch fast, subsequent switches slow"
// bug in demo-react. We measure wall-clock time from clicking a filter button
// to seeing the displayed notes match the new filter category.
test('demo-react: filter switching latency stays bounded across multiple switches', async ({ page }) => {
  const sessionEvents = [];
  // Watch for new BrowserChannel session establishment — these are the slow
  // ones (full reconnect handshake).
  page.on('request', req => {
    const u = req.url();
    if (u.includes('/Listen/channel') && !u.includes('SID=')) {
      sessionEvents.push({ t: Date.now(), kind: 'new-listen-session' });
    }
  });

  // Capture browser console for debugging if the test fails.
  page.on('console', msg => console.log(`[browser:${msg.type()}]`, msg.text().slice(0, 200)));
  page.on('pageerror', err => console.error('[browser:pageerror]', err.message));

  await page.goto('/');

  // Wait for Firebase to load and the initial 'all' snapshot to render.
  await page.waitForSelector('.controls', { timeout: 10_000 });

  // Seed at least one note in each category so every filter has something to show.
  // Add via the UI form. Use a unique tag per run so the visibility check
  // doesn't trip over notes left by a previous run.
  const tag = `seed-${Date.now()}`;
  const cats = ['general', 'idea', 'bug', 'todo'];
  for (const cat of cats) {
    await page.fill('.add-form input[placeholder="New note…"]', `${tag}-${cat}`);
    await page.selectOption('.add-form select', cat);
    await page.click('.add-form button[type="submit"]');
    await page.locator('.note-item', { hasText: `${tag}-${cat}` }).waitFor({ state: 'visible', timeout: 4_000 });
  }

  // Helper: click filter button `cat` and measure how long until at least one
  // note with that category badge is visible AND no note from any other
  // category remains.
  async function switchFilter(cat) {
    const start = Date.now();
    await page.locator('.controls').getByRole('button', { name: cat, exact: true }).click();
    await page.waitForFunction(
      (target) => {
        const items = Array.from(document.querySelectorAll('.note-item'));
        if (items.length === 0) return false;
        return items.every(it => {
          const badge = it.querySelector('.cat-badge');
          return badge && badge.textContent.trim() === target;
        });
      },
      cat,
      { timeout: 5_000 },
    );
    return Date.now() - start;
  }

  const results = [];
  for (const cat of cats) {
    const dt = await switchFilter(cat);
    console.log(`switch → ${cat}: ${dt}ms`);
    results.push({ cat, ms: dt });
    // Brief pause to let the back-channel settle.
    await page.waitForTimeout(150);
  }

  // The first switch and every subsequent switch should be in the same ballpark.
  // If the demo is reconnecting WebChannel sessions (the bug), later switches
  // take 1-13s while the first is sub-second.
  const max = Math.max(...results.map(r => r.ms));
  const min = Math.min(...results.map(r => r.ms));
  const median = [...results.map(r => r.ms)].sort((a, b) => a - b)[Math.floor(results.length / 2)];
  console.log(`min=${min}ms median=${median}ms max=${max}ms`);
  console.log(`new-listen-session events during run: ${sessionEvents.length}`);

  // The hard threshold: every switch under 2 seconds. If subsequent switches
  // trigger WebChannel reconnect, max will exceed several seconds.
  expect(max, `slowest filter switch must be under 2s; results=${JSON.stringify(results)}`).toBeLessThan(2000);

  // Also assert we're not creating a new Listen session per switch.
  // We expect at most 1 new session for the initial load (StrictMode may
  // induce 1-2). Anything more = reconnect-per-switch.
  expect(
    sessionEvents.length,
    `should not create a new Listen session per filter switch; got ${sessionEvents.length}`,
  ).toBeLessThanOrEqual(2);
});

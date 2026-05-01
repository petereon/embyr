import { test, expect } from '@playwright/test';

// Reproduce the user's filter-switch slowness in isolation.
// Pre-seed via REST so no client-side Write stream is involved, then drive
// the demo's filter buttons and measure the time between click and snapshot.
test('demo-react: filter switch on the Listen stream stays bounded', async ({ page }) => {
  // Pre-seed via REST (Firestore REST: POST :commit) — bypasses the JS SDK
  // entirely so the issue can't be confounded with Write-stream behavior.
  const cats = ['general', 'idea', 'bug', 'todo'];
  for (const cat of cats) {
    const res = await page.request.post(
      'http://localhost:17081/v1/projects/demo/databases/(default)/documents:commit',
      {
        headers: { 'content-type': 'application/json' },
        data: JSON.stringify({
          writes: [{
            update: {
              name: `projects/demo/databases/(default)/documents/notes/seed-${cat}-${Date.now()}`,
              fields: {
                text: { stringValue: `seed-${cat}` },
                category: { stringValue: cat },
                tags: { arrayValue: { values: [] } },
                votes: { integerValue: '0' },
                createdAt: { timestampValue: new Date().toISOString() },
              },
            },
          }],
        }),
      },
    );
    expect(res.ok(), `seed ${cat} failed (${res.status()})`).toBeTruthy();
  }

  // Track new Listen sessions for diagnostics.
  const listenSessions = [];
  page.on('request', req => {
    const u = req.url();
    if (u.includes('/Listen/channel')) {
      const params = new URLSearchParams(u.split('?')[1] || '');
      console.log(`[net:req] ${req.method()} RID=${params.get('RID')} SID=${params.get('SID')?.slice(0,8)} AID=${params.get('AID')} CI=${params.get('CI')} TYPE=${params.get('TYPE')}`);
    }
    if (u.includes('/Listen/channel') && req.method() === 'POST' && !u.includes('SID=')) {
      listenSessions.push({ t: Date.now(), url: u });
    }
  });
  page.on('response', res => {
    const u = res.url();
    if (u.includes('/Listen/channel')) {
      const params = new URLSearchParams(u.split('?')[1] || '');
      console.log(`[net:res] ${res.status()} RID=${params.get('RID')} SID=${params.get('SID')?.slice(0,8)}`);
    }
  });
  page.on('console', msg => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      console.log(`[browser:${msg.type()}]`, msg.text().slice(0, 200));
    }
  });

  await page.goto('/');
  await page.waitForSelector('.controls', { timeout: 10_000 });
  // Wait for the initial 'all' snapshot to render at least one note.
  await page.waitForSelector('.note-item', { timeout: 5_000 });

  async function switchFilter(cat) {
    const start = Date.now();
    await page.locator('.controls').getByRole('button', { name: cat, exact: true }).click();
    await page.waitForFunction(
      target => {
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
    console.log(`[${new Date().toISOString()}] switch → ${cat}: ${dt}ms`);
    results.push({ cat, ms: dt });
    await page.waitForTimeout(150);
  }

  console.log(`\nresults: ${JSON.stringify(results)}`);
  console.log(`new listen sessions during run: ${listenSessions.length}`);

  const max = Math.max(...results.map(r => r.ms));
  expect(max, `slowest switch must be < 2s; results=${JSON.stringify(results)}`).toBeLessThan(2000);
  expect(
    listenSessions.length,
    `should not create a Listen session per filter switch; got ${listenSessions.length}`,
  ).toBeLessThanOrEqual(2);
});

import { defineConfig, devices } from '@playwright/test';

const VITE_PORT = parseInt(process.env.VITE_PORT ?? '5174', 10);

export default defineConfig({
  testDir: './tests',
  timeout: 20_000,
  use: {
    baseURL: `http://localhost:${VITE_PORT}`,
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
  webServer: {
    command: `node ../../demo-react/node_modules/.bin/vite ../../demo-react --port ${VITE_PORT}`,
    url: `http://localhost:${VITE_PORT}/test-harness.html`,
    reuseExistingServer: true,
    timeout: 30_000,
  },
});

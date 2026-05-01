import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globalSetup: './setup.js',
    // happy-dom gives the Firebase SDK a browser-like environment for APIs
    // like XMLHttpRequest. Write operations use gRPC (pointed at grpcPort);
    // Listen operations use gRPC via the same connection.
    environment: 'happy-dom',
    testTimeout: 15000,
    hookTimeout: 30000,
    reporters: ['verbose'],
  },
});

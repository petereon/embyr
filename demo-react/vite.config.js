import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // REST (grpc-gateway): used by firebase/firestore/lite
      '/v1': {
        target: 'http://localhost:17081',
        changeOrigin: true,
      },
      // gRPC-Web: used by the full firebase/firestore SDK
      '/google.firestore.v1.Firestore': {
        target: 'http://localhost:17081',
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on('proxyReq', (proxyReq, req) => {
            console.log('[proxy]', req.method, req.url);
          });
          proxy.on('error', (err, req) => {
            console.error('[proxy error]', req.method, req.url, err.message);
          });
        },
      },
    },
  },
});

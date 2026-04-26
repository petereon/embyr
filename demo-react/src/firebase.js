import { initializeApp } from 'firebase/app';
import { getFirestore, connectFirestoreEmulator } from 'firebase/firestore';

const app = initializeApp({
  projectId: 'demo',
  apiKey: 'demo-key',
  authDomain: 'localhost',
});

export const db = getFirestore(app);

// Vite proxies /v1/* (REST) and /google.firestore.v1.Firestore/* (gRPC-Web + BrowserChannel)
// to http://localhost:17081 — firstyr handles both transports.
// Connect directly to the Go server, bypassing Vite proxy which buffers
// chunked responses and delays BrowserChannel session ID delivery.
connectFirestoreEmulator(db, 'localhost', 17081);

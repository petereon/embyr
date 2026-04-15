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
connectFirestoreEmulator(db, 'localhost', 5173);

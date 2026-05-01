import { initializeApp, deleteApp } from 'firebase/app';
import {
  getFirestore,
  connectFirestoreEmulator,
  onSnapshot,
  collection,
  query,
  where,
  orderBy,
  addDoc,
  getDocs,
  setDoc,
  doc,
  terminate,
} from 'firebase/firestore';

const REST_PORT = 17081;

function makeTestDb() {
  const app = initializeApp(
    { projectId: 'demo', apiKey: 'demo-key', authDomain: 'localhost' },
    crypto.randomUUID(),
  );
  const db = getFirestore(app);
  connectFirestoreEmulator(db, 'localhost', REST_PORT);
  return { app, db };
}

async function closeTestDb({ app, db }) {
  await terminate(db);
  await deleteApp(app);
}

function col() {
  return 'test-' + crypto.randomUUID().slice(0, 8);
}

window.__testHarness = {
  makeTestDb,
  closeTestDb,
  col,
  onSnapshot,
  collection,
  query,
  where,
  orderBy,
  addDoc,
  getDocs,
  setDoc,
  doc,
};

window.__testHarnessReady = true;

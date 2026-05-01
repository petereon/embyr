import { initializeApp, deleteApp } from "firebase/app";
import {
  initializeFirestore,
  connectFirestoreEmulator,
  terminate,
} from "firebase/firestore";

/**
 * Create an isolated Firestore client connected to the local embyr server.
 *
 * In Node.js the Firebase SDK uses native gRPC (HTTP/2) for both the Watch
 * (Listen) and Write bidirectional streams. We point connectFirestoreEmulator
 * at embyr's gRPC port so both streams work correctly.
 */
export function makeDb() {
  const appName = crypto.randomUUID();
  const app = initializeApp({ projectId: "p" }, appName);
  const db = initializeFirestore(app, {});
  connectFirestoreEmulator(
    db,
    "127.0.0.1",
    parseInt(process.env.EMBYR_GRPC_PORT, 10),
  );
  return { app, db };
}

export async function closeDb({ app, db }) {
  await terminate(db);
  await deleteApp(app);
}

/** Returns a unique collection name so tests don't share documents. */
export function col() {
  return "col-" + crypto.randomUUID().slice(0, 8);
}

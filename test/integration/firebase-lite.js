import { initializeApp, deleteApp } from "firebase/app";
import {
  getFirestore,
  connectFirestoreEmulator,
} from "firebase/firestore/lite";

/**
 * Create an isolated Firestore Lite client connected to the local embyr REST port.
 *
 * The Lite SDK uses plain HTTP REST for all operations — no gRPC, no BrowserChannel.
 * This exercises embyr's grpc-gateway REST transport path.
 */
export function makeDb() {
  const appName = crypto.randomUUID();
  const app = initializeApp({ projectId: "p" }, appName);
  const db = getFirestore(app);
  connectFirestoreEmulator(
    db,
    "127.0.0.1",
    parseInt(process.env.EMBYR_REST_PORT, 10),
  );
  return { app, db };
}

export async function closeDb({ app }) {
  await deleteApp(app);
}

/** Returns a unique collection name so tests don't share documents. */
export function col() {
  return "col-" + crypto.randomUUID().slice(0, 8);
}

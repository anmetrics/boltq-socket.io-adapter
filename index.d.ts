import { Adapter } from "socket.io-adapter";
import type { Namespace } from "socket.io";

export interface BoltQAdapterOptions {
  /** BoltQ server host. Default: "127.0.0.1" */
  host?: string;
  /** BoltQ HTTP port (WebSocket at /ws). Default: 9090 */
  port?: number;
  /** Channel prefix for pub/sub topics. Default: "socket.io" */
  key?: string;
  /** API key for BoltQ authentication. Default: "" */
  apiKey?: string;
  /** Timeout for inter-server requests in ms. Default: 5000 */
  requestsTimeout?: number;
  /** Interval between reconnection attempts in ms. Default: 2000 */
  reconnectInterval?: number;
}

/**
 * Create a BoltQ adapter factory for Socket.IO.
 *
 * @example
 * ```js
 * import { Server } from "socket.io";
 * import { createAdapter } from "@boltq/socket.io-adapter";
 *
 * const io = new Server(3000);
 * io.adapter(createAdapter({ host: "127.0.0.1", port: 9090 }));
 * ```
 */
export function createAdapter(
  opts?: BoltQAdapterOptions
): (nsp: Namespace) => BoltQAdapter;

/**
 * BoltQ-backed Socket.IO Adapter.
 * Extends the base Adapter to broadcast events via BoltQ pub/sub.
 */
export class BoltQAdapter extends Adapter {
  readonly uid: string;
  readonly config: Required<BoltQAdapterOptions>;

  constructor(nsp: Namespace, config: Required<BoltQAdapterOptions>);

  close(): void;
}

export default createAdapter;

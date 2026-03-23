/**
 * @boltq/socket.io-adapter
 *
 * Socket.IO adapter backed by BoltQ message queue.
 * Replaces @socket.io/redis-adapter for scaling Socket.IO across multiple servers.
 *
 * Uses BoltQ WebSocket for pub/sub communication between servers.
 * Zero external dependencies beyond socket.io-adapter base class.
 */

import { Adapter } from "socket.io-adapter";
import { randomBytes } from "crypto";

const DEFAULTS = {
  host: "127.0.0.1",
  port: 9090,
  key: "socket.io",
  apiKey: "",
  requestsTimeout: 5000,
  reconnectInterval: 2000,
};

// Inter-server request types (matches @socket.io/redis-adapter).
const RequestType = {
  SOCKETS: 0,
  ALL_ROOMS: 1,
  REMOTE_JOIN: 2,
  REMOTE_LEAVE: 3,
  REMOTE_DISCONNECT: 4,
  REMOTE_FETCH: 5,
  SERVER_SIDE_EMIT: 6,
  BROADCAST: 7,
  BROADCAST_ACK: 8,
};

/**
 * Create a BoltQ adapter factory for Socket.IO.
 *
 * @param {object} opts
 * @param {string} [opts.host="127.0.0.1"] BoltQ server host
 * @param {number} [opts.port=9090] BoltQ HTTP port (WebSocket at /ws)
 * @param {string} [opts.key="socket.io"] Channel prefix
 * @param {string} [opts.apiKey=""] API key for BoltQ authentication
 * @param {number} [opts.requestsTimeout=5000] Timeout for inter-server requests (ms)
 * @returns {function} Adapter class constructor for socket.io
 */
export function createAdapter(opts = {}) {
  const config = { ...DEFAULTS, ...opts };

  return function (nsp) {
    return new BoltQAdapter(nsp, config);
  };
}

/**
 * BoltQ-backed Socket.IO Adapter.
 *
 * Architecture:
 *   Socket.IO Server 1 ──publish──▶ BoltQ ──subscribe──▶ Socket.IO Server 2
 *   Socket.IO Server 2 ──publish──▶ BoltQ ──subscribe──▶ Socket.IO Server 1
 *
 * Each server subscribes to:
 *   - Broadcast channel: `{key}#{namespace}#` (all events)
 *   - Request channel:   `{key}-request#{namespace}#` (inter-server RPC)
 *   - Response channel:  `{key}-response#{uid}#` (directed responses)
 */
export class BoltQAdapter extends Adapter {
  /**
   * @param {import("socket.io").Namespace} nsp
   * @param {object} config
   */
  constructor(nsp, config) {
    super(nsp);

    this.uid = randomBytes(6).toString("hex");
    this.config = config;
    this.key = config.key;
    this.requestsTimeout = config.requestsTimeout;

    /** @type {WebSocket|null} */
    this._ws = null;
    this._connected = false;
    this._reconnectTimer = null;

    // Pending inter-server requests.
    /** @type {Map<string, {resolve: Function, reject: Function, timer: NodeJS.Timeout, responses: any[]}>} */
    this._requests = new Map();

    // Channel names.
    this._broadcastChannel = `${this.key}#${nsp.name}#`;
    this._requestChannel = `${this.key}-request#${nsp.name}#`;
    this._responseChannel = `${this.key}-response#${this.uid}#`;

    this._connect();
  }

  // --- Connection Management ---

  _connect() {
    const url = `ws://${this.config.host}:${this.config.port}/ws`;

    // Use globalThis.WebSocket for browser or Node 21+, otherwise dynamic import.
    const WS = globalThis.WebSocket;
    if (!WS) {
      // Fallback: raw TCP via net.Socket (non-browser environments).
      this._connectRawWS(url);
      return;
    }

    this._ws = new WS(url);
    this._ws.onopen = () => this._onOpen();
    this._ws.onmessage = (ev) => this._onMessage(typeof ev.data === "string" ? ev.data : ev.data.toString());
    this._ws.onclose = () => this._onClose();
    this._ws.onerror = (err) => this._onError(err);
  }

  /**
   * Raw WebSocket connection for Node.js < 21 environments.
   * Uses net.Socket with manual WebSocket handshake.
   */
  async _connectRawWS(url) {
    try {
      // Try dynamic import of 'ws' package first (most Node.js projects have it via socket.io).
      const { default: WS } = await import("ws");
      this._ws = new WS(url);
      this._ws.on("open", () => this._onOpen());
      this._ws.on("message", (data) => this._onMessage(data.toString()));
      this._ws.on("close", () => this._onClose());
      this._ws.on("error", (err) => this._onError(err));
    } catch {
      console.error(
        "[@boltq/socket.io-adapter] WebSocket not available. " +
        "Install 'ws' package or use Node.js >= 21."
      );
    }
  }

  _onOpen() {
    this._connected = true;
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }

    // Authenticate if needed.
    if (this.config.apiKey) {
      this._send({ cmd: "auth", api_key: this.config.apiKey });
    }

    // Subscribe to broadcast, request, and response channels.
    this._send({ cmd: "subscribe", topic: this._broadcastChannel, id: this.uid, durable: false });
    this._send({ cmd: "subscribe", topic: this._requestChannel, id: this.uid, durable: false });
    this._send({ cmd: "subscribe", topic: this._responseChannel, id: this.uid, durable: false });
  }

  _onMessage(raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    // Subscription push events.
    if (msg.event === "message") {
      this._handlePush(msg);
      return;
    }
    // Command responses (auth, subscribe confirmations) — ignore.
  }

  _onClose() {
    this._connected = false;
    this._scheduleReconnect();
  }

  _onError(err) {
    // Suppress — onClose will fire next.
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      this._connect();
    }, this.config.reconnectInterval);
  }

  _send(obj) {
    if (this._ws && this._connected) {
      const data = typeof this._ws.send === "function"
        ? JSON.stringify(obj)
        : JSON.stringify(obj);
      try {
        this._ws.send(data);
      } catch {
        // Connection may have dropped.
      }
    }
  }

  /**
   * Publish a message to a BoltQ pub/sub topic.
   */
  _publish(channel, data) {
    this._send({
      cmd: "publish_topic",
      topic: channel,
      payload: data,
    });
  }

  // --- Adapter Overrides ---

  /**
   * Called when the adapter is no longer needed.
   */
  close() {
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
    }
    if (this._ws) {
      try { this._ws.close(); } catch {}
      this._ws = null;
    }
    this._connected = false;

    // Clear pending requests.
    for (const [, req] of this._requests) {
      clearTimeout(req.timer);
      req.reject(new Error("adapter closed"));
    }
    this._requests.clear();
  }

  /**
   * Broadcast a packet to matching sockets.
   * This is the core method — publishes to BoltQ so all servers receive it.
   */
  broadcast(packet, opts) {
    // Always deliver locally first.
    super.broadcast(packet, opts);

    // Publish to BoltQ for other servers.
    const msg = {
      type: RequestType.BROADCAST,
      uid: this.uid,
      packet,
      opts: {
        rooms: opts.rooms ? [...opts.rooms] : [],
        except: opts.except ? [...opts.except] : [],
        flags: opts.flags || {},
      },
    };

    // If targeting specific rooms, publish per-room for efficiency.
    // Otherwise publish to the global broadcast channel.
    this._publish(this._broadcastChannel, msg);
  }

  /**
   * Handle incoming pub/sub messages from BoltQ.
   */
  _handlePush(msg) {
    const topic = msg.topic;
    let payload;

    try {
      // payload may be double-encoded if BoltQ wraps it.
      payload = typeof msg.payload === "string" ? JSON.parse(msg.payload) : msg.payload;
    } catch {
      payload = msg.payload;
    }

    if (!payload || !payload.type) return;

    // Ignore own messages.
    if (payload.uid === this.uid) return;

    if (topic === this._broadcastChannel) {
      this._onBroadcast(payload);
    } else if (topic === this._requestChannel) {
      this._onRequest(payload);
    } else if (topic === this._responseChannel) {
      this._onResponse(payload);
    }
  }

  /**
   * Handle broadcast from another server.
   */
  _onBroadcast(msg) {
    if (msg.type !== RequestType.BROADCAST) return;

    const packet = msg.packet;
    const opts = {
      rooms: msg.opts.rooms ? new Set(msg.opts.rooms) : new Set(),
      except: msg.opts.except ? new Set(msg.opts.except) : new Set(),
      flags: msg.opts.flags || {},
    };

    // Deliver to local sockets matching the criteria.
    super.broadcast(packet, opts);
  }

  // --- Inter-Server RPC ---

  /**
   * Send a request to all other servers and collect responses.
   * @returns {Promise<any[]>}
   */
  _sendRequest(type, data = {}) {
    return new Promise((resolve, reject) => {
      const requestId = randomBytes(8).toString("hex");

      const timer = setTimeout(() => {
        const req = this._requests.get(requestId);
        this._requests.delete(requestId);
        if (req) {
          resolve(req.responses);
        }
      }, this.requestsTimeout);

      this._requests.set(requestId, { resolve, reject, timer, responses: [] });

      this._publish(this._requestChannel, {
        type,
        requestId,
        uid: this.uid,
        responseChannel: this._responseChannel,
        ...data,
      });
    });
  }

  /**
   * Handle a request from another server.
   */
  _onRequest(msg) {
    const { type, requestId, uid, responseChannel } = msg;
    if (uid === this.uid) return; // ignore own

    let responseData;

    switch (type) {
      case RequestType.SOCKETS: {
        const sockets = this._getLocalSockets(msg.opts);
        responseData = { sockets };
        break;
      }

      case RequestType.ALL_ROOMS: {
        responseData = { rooms: [...this.rooms.keys()] };
        break;
      }

      case RequestType.REMOTE_JOIN: {
        const socket = this.nsp.sockets.get(msg.sid);
        if (socket) {
          for (const room of msg.rooms) {
            socket.join(room);
          }
        }
        responseData = {};
        break;
      }

      case RequestType.REMOTE_LEAVE: {
        const socket = this.nsp.sockets.get(msg.sid);
        if (socket) {
          for (const room of msg.rooms) {
            socket.leave(room);
          }
        }
        responseData = {};
        break;
      }

      case RequestType.REMOTE_DISCONNECT: {
        const socket = this.nsp.sockets.get(msg.sid);
        if (socket) {
          socket.disconnect(msg.close);
        }
        responseData = {};
        break;
      }

      case RequestType.REMOTE_FETCH: {
        const sockets = this._getLocalSocketDetails(msg.opts);
        responseData = { sockets };
        break;
      }

      case RequestType.SERVER_SIDE_EMIT: {
        // Emit the event on this server's namespace.
        const args = msg.args || [];
        const event = args[0];
        if (event) {
          this.nsp.emit(event, ...args.slice(1));
        }
        responseData = {};
        break;
      }

      default:
        return;
    }

    // Send response back.
    this._publish(responseChannel, {
      type,
      requestId,
      uid: this.uid,
      data: responseData,
    });
  }

  /**
   * Handle a response to our request from another server.
   */
  _onResponse(msg) {
    const { requestId, data } = msg;
    const req = this._requests.get(requestId);
    if (!req) return;

    req.responses.push(data);
  }

  // --- Distributed Socket Operations ---

  /**
   * Returns all sockets across all servers that match the given options.
   * @override
   */
  async sockets(rooms) {
    const localSockets = await super.sockets(rooms);
    const opts = { rooms: rooms ? [...rooms] : [] };

    const responses = await this._sendRequest(RequestType.SOCKETS, { opts });

    const allSockets = new Set(localSockets);
    for (const resp of responses) {
      if (resp && resp.sockets) {
        for (const sid of resp.sockets) {
          allSockets.add(sid);
        }
      }
    }
    return allSockets;
  }

  /**
   * Returns all rooms across all servers.
   * @override
   */
  async allRooms() {
    const localRooms = new Set(this.rooms.keys());

    const responses = await this._sendRequest(RequestType.ALL_ROOMS);

    for (const resp of responses) {
      if (resp && resp.rooms) {
        for (const room of resp.rooms) {
          localRooms.add(room);
        }
      }
    }
    return localRooms;
  }

  /**
   * Fetch sockets across all servers.
   * @override
   */
  async fetchSockets(opts) {
    const localSockets = await super.fetchSockets(opts);

    if (opts.flags && opts.flags.local) {
      return localSockets;
    }

    const responses = await this._sendRequest(RequestType.REMOTE_FETCH, {
      opts: {
        rooms: opts.rooms ? [...opts.rooms] : [],
        except: opts.except ? [...opts.except] : [],
      },
    });

    const remoteSockets = [];
    for (const resp of responses) {
      if (resp && resp.sockets) {
        for (const s of resp.sockets) {
          remoteSockets.push(new RemoteSocket(this, s));
        }
      }
    }

    return [...localSockets, ...remoteSockets];
  }

  /**
   * Make matching sockets join rooms across all servers.
   * @override
   */
  async addSockets(opts, rooms) {
    await super.addSockets(opts, rooms);

    if (opts.flags && opts.flags.local) return;

    // Tell other servers.
    for (const sid of this._matchingSids(opts)) {
      this._publish(this._requestChannel, {
        type: RequestType.REMOTE_JOIN,
        uid: this.uid,
        sid,
        rooms: [...rooms],
      });
    }
  }

  /**
   * Make matching sockets leave rooms across all servers.
   * @override
   */
  async delSockets(opts, rooms) {
    await super.delSockets(opts, rooms);

    if (opts.flags && opts.flags.local) return;

    for (const sid of this._matchingSids(opts)) {
      this._publish(this._requestChannel, {
        type: RequestType.REMOTE_LEAVE,
        uid: this.uid,
        sid,
        rooms: [...rooms],
      });
    }
  }

  /**
   * Disconnect matching sockets across all servers.
   * @override
   */
  async disconnectSockets(opts, close) {
    await super.disconnectSockets(opts, close);

    if (opts.flags && opts.flags.local) return;

    for (const sid of this._matchingSids(opts)) {
      this._publish(this._requestChannel, {
        type: RequestType.REMOTE_DISCONNECT,
        uid: this.uid,
        sid,
        close,
      });
    }
  }

  /**
   * Send an event to other Socket.IO servers.
   * @override
   */
  serverSideEmit(packet) {
    const args = Array.isArray(packet) ? packet : [packet];
    this._publish(this._requestChannel, {
      type: RequestType.SERVER_SIDE_EMIT,
      uid: this.uid,
      args,
    });
  }

  // --- Helpers ---

  _getLocalSockets(opts) {
    const rooms = opts && opts.rooms ? new Set(opts.rooms) : null;
    const sockets = [];

    if (rooms && rooms.size > 0) {
      for (const room of rooms) {
        const roomSet = this.rooms.get(room);
        if (roomSet) {
          for (const sid of roomSet) {
            if (!sockets.includes(sid)) {
              sockets.push(sid);
            }
          }
        }
      }
    } else {
      for (const sid of this.sids.keys()) {
        sockets.push(sid);
      }
    }

    return sockets;
  }

  _getLocalSocketDetails(opts) {
    const sids = this._getLocalSockets(opts);
    const details = [];

    for (const sid of sids) {
      const socket = this.nsp.sockets.get(sid);
      if (socket) {
        details.push({
          id: socket.id,
          handshake: socket.handshake,
          rooms: [...socket.rooms],
          data: socket.data,
        });
      }
    }

    return details;
  }

  _matchingSids(opts) {
    const result = [];
    const rooms = opts.rooms ? new Set(opts.rooms) : null;
    const except = opts.except ? new Set(opts.except) : new Set();

    if (rooms && rooms.size > 0) {
      for (const room of rooms) {
        const roomSet = this.rooms.get(room);
        if (roomSet) {
          for (const sid of roomSet) {
            if (!except.has(sid) && !result.includes(sid)) {
              result.push(sid);
            }
          }
        }
      }
    } else {
      for (const sid of this.sids.keys()) {
        if (!except.has(sid)) {
          result.push(sid);
        }
      }
    }

    return result;
  }
}

/**
 * Remote socket proxy for distributed fetchSockets().
 * Provides a socket-like interface for sockets on other servers.
 */
class RemoteSocket {
  constructor(adapter, details) {
    this.id = details.id;
    this.handshake = details.handshake || {};
    this.rooms = new Set(details.rooms || []);
    this.data = details.data || {};
    this._adapter = adapter;
  }

  join(rooms) {
    const roomArr = Array.isArray(rooms) ? rooms : [rooms];
    this._adapter._publish(this._adapter._requestChannel, {
      type: RequestType.REMOTE_JOIN,
      uid: this._adapter.uid,
      sid: this.id,
      rooms: roomArr,
    });
    for (const r of roomArr) this.rooms.add(r);
    return Promise.resolve();
  }

  leave(room) {
    this._adapter._publish(this._adapter._requestChannel, {
      type: RequestType.REMOTE_LEAVE,
      uid: this._adapter.uid,
      sid: this.id,
      rooms: [room],
    });
    this.rooms.delete(room);
    return Promise.resolve();
  }

  disconnect(close = false) {
    this._adapter._publish(this._adapter._requestChannel, {
      type: RequestType.REMOTE_DISCONNECT,
      uid: this._adapter.uid,
      sid: this.id,
      close,
    });
    return Promise.resolve();
  }

  emit() {
    throw new Error("Cannot emit to a remote socket — use io.to(room).emit() instead");
  }
}

export default createAdapter;

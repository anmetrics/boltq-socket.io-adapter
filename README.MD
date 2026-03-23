# @boltq/socket.io-adapter

Socket.IO adapter backed by [BoltQ](https://github.com/boltq/boltq) message queue. Scale Socket.IO across multiple servers using BoltQ pub/sub instead of Redis.

## Architecture

```
                    BoltQ Server
                   (Pub/Sub + WAL)
                    ▲          │
         publish    │          │  subscribe
              ┌─────┘          └─────┐
              │                      │
    Socket.IO Server 1     Socket.IO Server 2
         │                      │
    ┌────┴────┐            ┌────┴────┐
    │ Clients │            │ Clients │
    └─────────┘            └─────────┘
```

## Why BoltQ over Redis?

| Feature | Redis Adapter | BoltQ Adapter |
|---------|--------------|---------------|
| Latency | ~0.1-0.5ms | ~0.05ms |
| Memory footprint | 50-100MB | <50MB |
| Persistence | No (Pub/Sub is fire-and-forget) | Optional (WAL) |
| Dependencies | Redis server | BoltQ server (single binary) |
| Clustering | Redis Cluster | Built-in Raft |
| Protocol | TCP (RESP) | WebSocket (JSON) |

## Installation

```bash
npm install @boltq/socket.io-adapter
```

## Quick Start

```js
import { Server } from "socket.io";
import { createAdapter } from "@boltq/socket.io-adapter";

const io = new Server(3000);

// Connect to BoltQ server
io.adapter(createAdapter({
  host: "127.0.0.1",
  port: 9090,           // BoltQ HTTP port (WebSocket at /ws)
}));

// That's it! Events are now broadcast across all servers.
io.on("connection", (socket) => {
  socket.on("chat", (msg) => {
    io.emit("chat", msg); // Delivered to ALL clients on ALL servers
  });
});
```

## Configuration

```js
createAdapter({
  host: "127.0.0.1",       // BoltQ server host
  port: 9090,              // BoltQ HTTP port
  key: "socket.io",        // Channel prefix (namespace isolation)
  apiKey: "",              // BoltQ API key (if auth enabled)
  requestsTimeout: 5000,   // Inter-server request timeout (ms)
  reconnectInterval: 2000, // Reconnect delay on disconnect (ms)
});
```

## Multi-Server Example

```js
// server-1.js (port 3001)
import { Server } from "socket.io";
import { createAdapter } from "@boltq/socket.io-adapter";

const io = new Server(3001);
io.adapter(createAdapter({ host: "boltq-host", port: 9090 }));

io.on("connection", (socket) => {
  socket.on("message", (data) => {
    // This reaches clients on server-2 too
    io.to("room1").emit("message", data);
  });
});
```

```js
// server-2.js (port 3002)
import { Server } from "socket.io";
import { createAdapter } from "@boltq/socket.io-adapter";

const io = new Server(3002);
io.adapter(createAdapter({ host: "boltq-host", port: 9090 }));

io.on("connection", (socket) => {
  socket.join("room1");
});
```

## Features

### Broadcasting
Events emitted to rooms are delivered to all matching clients across all servers.

```js
io.to("room1").emit("event", data);        // Room broadcast
io.except("room2").emit("event", data);     // Broadcast with exclusion
socket.broadcast.emit("event", data);       // All except sender
```

### Distributed Socket Operations

```js
// Fetch all sockets across all servers
const sockets = await io.fetchSockets();

// Get all rooms across all servers
const rooms = await io.adapter.allRooms();

// Make remote sockets join a room
await io.in("room1").socketsJoin("room2");

// Disconnect remote sockets
await io.in("room1").disconnectSockets(true);
```

### Server-to-Server Communication

```js
// Send events between Socket.IO servers (not to clients)
io.serverSideEmit("deploy", { version: "1.2.3" });

io.on("deploy", (data) => {
  console.log("Deploy event from another server:", data);
});
```

### Automatic Reconnection

The adapter automatically reconnects to BoltQ if the connection drops, with configurable retry interval.

## How It Works

1. **Broadcast**: When a server emits an event, it delivers locally AND publishes to BoltQ pub/sub topic
2. **Receive**: All other servers subscribed to the same topic receive the event and deliver to their local clients
3. **Rooms**: Room membership is tracked locally per server. Cross-server room operations use an RPC pattern via BoltQ pub/sub
4. **RPC**: Distributed operations (fetchSockets, allRooms, etc.) use a request/response pattern with timeout

## Channel Structure

| Channel | Purpose |
|---------|---------|
| `socket.io#/# ` | Global broadcast for default namespace |
| `socket.io#/chat#` | Broadcast for `/chat` namespace |
| `socket.io-request#/#` | Inter-server RPC requests |
| `socket.io-response#<uid>#` | Directed RPC responses |

## Requirements

- BoltQ server >= 1.0.0
- Socket.IO >= 4.0.0
- Node.js >= 18 (or `ws` package for WebSocket support)

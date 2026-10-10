const kv = await Deno.openKv();

const CORS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS, HEAD, DELETE",
  "Access-Control-Allow-Headers": "Content-Type, X-Client-Id, X-Peer-Id, X-Client-Name, X-Room-Secret, X-Signature",
  "Access-Control-Max-Age":       "86400",
};

const ARCHIVES_TOTAL_DOORS   = 1300;
const ARCHIVES_SECTORS       = 26;
const ARCHIVES_DOORS_PER_SEC = 50;
const ROOMS_TOTAL_DOORS      = 1000;

const MAX_BODY_BYTES          = 262144;
const MAX_BODY_OVERFLOW_BYTES = 1048576;
const MAX_ROOM_LEN            = 128;
const MAX_CLIENT_LEN          = 64;
const MAX_NAME_LEN            = 32;
const MAX_CHANNEL_LEN         = 32;
const MSG_TTL_MS              = 600_000;
const MAX_MESSAGES_POLL       = 1000;
const RATE_WINDOW_MS          = 1000;
const RATE_MAX_REQUESTS       = 300;
const RATE_OVERFLOW_TRIGGER   = 60;
const RATE_OVERFLOW_EXIT      = 20;
const OVERFLOW_EXIT_HOLD_MS   = 5_000;
const CONTROL_RATE_MS         = 250;
const ROOM_IDLE_MS            = 60_000;
const KICK_COOLDOWN_MS        = 2_000;
const BAN_TTL_MS              = 3_600_000;
const CONTROL_LOG_MAX         = 200;
const RPC_TIMEOUT_MS          = 30_000;
const MAX_CAPS_PER_PEER       = 256;
const MAX_RPC_PENDING         = 128;
const MAX_STRING_LEN          = 16384;
const MAX_DOOR_BATCH          = 500;
const DOOR_TTL_MS             = 3_600_000;

const CHUNK_SIZE              = 32768;
const MAX_CHUNKS              = 64;
const CHUNK_TIMEOUT_MS        = 60_000;
const OVERFLOW_BUFFER_MAX     = 5000;
const OVERFLOW_TTL_MS         = 900_000;
const OVERFLOW_DRAIN_BATCH    = 100;

const GAMEFLOW_MAX_DURATION_MS = 30_000;
const GAMEFLOW_MIN_DURATION_MS = 1_000;
const GAMEFLOW_MAX_INTENSITY   = 10;
const GAMEFLOW_MIN_INTENSITY   = 1;
const GAMEFLOW_SYNC_LEAD_MS    = 500;
const GAMEFLOW_READY_TIMEOUT   = 5_000;
const GAMEFLOW_TICK_WINDOW_MS  = 1_000;
const GAMEFLOW_MAX_TICK_RATE   = 500;

const CONTROL_CMDS = new Set([
  "@KICK", "@BAN", "@UNBAN", "@MUTE", "@UNMUTE",
  "@FREEZE", "@UNFREEZE", "@PROMOTE", "@DEMOTE",
  "@TRANSFER", "@PING", "@RESET", "@KILL", "@BROADCAST",
]);

const RPC_CMDS = new Set(["@CALL", "@RETURN", "@ERROR"]);

const HOST_ONLY = new Set([
  "@UNBAN", "@PROMOTE", "@DEMOTE", "@TRANSFER", "@RESET",
]);

const DOOR_CMDS = new Set(["@DOOR_OPEN", "@DOOR_CLOSE", "@DOOR_LOCK", "@DOOR_SYNC", "@DOOR_RESET"]);

const OVERFLOW_CMDS = new Set([
  "@OVERFLOW_STATUS", "@OVERFLOW_DRAIN", "@OVERFLOW_CLEAR",
  "@OVERFLOW_RESUME", "@OVERFLOW_STATS",
]);

const CHUNK_CMDS = new Set(["@CHUNK_BEGIN", "@CHUNK", "@CHUNK_END", "@CHUNK_ABORT"]);

const GAMEFLOW_CMDS = new Set([
  "@GAME_OVERFLOW_BEGIN", "@GAME_OVERFLOW_READY", "@GAME_OVERFLOW_GO",
  "@GAME_OVERFLOW_STOP", "@GAME_OVERFLOW_STATUS", "@GAME_OVERFLOW_TICK",
  "@GAME_OVERFLOW_EXTEND", "@GAME_OVERFLOW_KICK_PEER",
]);

type DoorState = {
  sector: string;
  index: number;
  opened: boolean;
  locked: boolean;
  openedBy?: string;
  openedAt?: number;
  updatedAt: number;
};

type PeerCapability = {
  name: string;
  args: string[];
  desc?: string;
  registeredAt: number;
};

type OverflowState = {
  active: boolean;
  buffered: number;
  lastTrigger: number;
  lastExitCheck: number;
  totalRouted: number;
  totalDrained: number;
  totalDropped: number;
  mode: "normal" | "warn" | "overflow" | "critical";
};

type ChunkBuffer = {
  id: string;
  cmd: string;
  target: string;
  channel: string;
  total: number;
  received: number;
  pieces: Map<number, string>;
  startedAt: number;
  lastAt: number;
  meta: Record<string, unknown>;
};

type GameOverflowMode = "burst" | "sustain" | "pulse" | "wave";

type GameOverflowSession = {
  id: string;
  mode: GameOverflowMode;
  target: string;
  initiatedBy: string;
  participants: Set<string>;
  ready: Set<string>;
  startedAt: number;
  stopAt: number;
  durationMs: number;
  intensity: number;
  active: boolean;
  syncEpoch: number;
  stats: {
    totalTicks: number;
    perPeer: Map<string, number>;
    peakRate: number;
  };
};

type Peer = {
  id: string;
  name: string;
  joinedAt: number;
  lastSeen: number;
  isHost: boolean;
  muted: boolean;
  frozen: boolean;
  banned: boolean;
  lastKickAt: number;
  lastControlAt: number;
  sessionToken: string;
  capabilities: Map<string, PeerCapability>;
  rpcPending: Map<number, { target: string; ts: number; cmd: string }>;
  overflow: OverflowState;
  chunks: Map<string, ChunkBuffer>;
  recentMsgTimes: number[];
};

type Room = {
  peers: Map<string, Peer>;
  sockets: Map<string, WebSocket>;
  host: string | null;
  banned: Set<string>;
  bannedNames: Set<string>;
  openControl: boolean;
  roomSecret: string | null;
  createdAt: number;
  controlLog: Array<{ ts: number; from: string; cmd: string; to: string; ok: boolean; reason?: string }>;
  capabilityIndex: Map<string, Set<string>>;
  doorCache: Map<string, DoorState>;
  floorType: "hotel" | "archives" | "rooms" | "mines" | "unknown";
  maxDoorSeen: number;
  overflowLog: Array<{ ts: number; peer: string; buffered: number; drained: number }>;
  gameOverflow: GameOverflowSession | null;
};

type RateBucket = { count: number; reset: number };
type RpcCorrelation = { issuedBy: string; issuedTo: string; cmd: string; ts: number };

const rooms       = new Map<string, Room>();
const rateBuckets = new Map<string, RateBucket>();
const rpcRegistry = new Map<string, Map<number, RpcCorrelation>>();

function jsonResponse(
  data: unknown,
  status = 200,
  extra: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      ...CORS,
      "Content-Type":  "application/json; charset=utf-8",
      "Cache-Control": "no-store",
      ...extra,
    },
  });
}

function sanitizeRoom(raw: string): string | null {
  if (!raw || raw.length === 0 || raw.length > MAX_ROOM_LEN) return null;
  return /^[A-Za-z0-9_-]+$/.test(raw) ? raw : null;
}

function sanitizeClient(raw: string | null): string | null {
  if (!raw) return null;
  const c = raw.replace(/[^A-Za-z0-9_-]/g, "").slice(0, MAX_CLIENT_LEN);
  return c.length > 0 ? c : null;
}

function sanitizeName(raw: string | null): string {
  if (!raw) return "peer";
  return raw.replace(/[^\w\s.-]/g, "").slice(0, MAX_NAME_LEN) || "peer";
}

function sanitizeChannel(raw: unknown): string {
  if (typeof raw !== "string") return "default";
  return raw.replace(/[^A-Za-z0-9_.-]/g, "").slice(0, MAX_CHANNEL_LEN) || "default";
}

function sanitizeSector(raw: string): string | null {
  if (!raw) return null;
  const c = raw.toUpperCase().slice(0, 2);
  return /^[A-Z]{1,2}$/.test(c) ? c : null;
}

function isString(v: unknown, max = MAX_STRING_LEN): v is string {
  return typeof v === "string" && v.length <= max;
}

function isNumber(v: unknown): v is number {
  return typeof v === "number" && Number.isFinite(v);
}

function isStringArray(v: unknown): v is string[] {
  if (!Array.isArray(v)) return false;
  if (v.length > 64) return false;
  return v.every((x) => typeof x === "string" && x.length <= 256);
}

function isValidSector(s: string): boolean {
  return /^[A-Z]$/.test(s) || /^[A-Z]{2}$/.test(s);
}

function doorKey(sector: string, index: number): string {
  return `${sector}-${String(index).padStart(3, "0")}`;
}

function pruneRateBuckets(): void {
  if (rateBuckets.size < 8000) return;
  const now = Date.now();
  for (const [id, b] of rateBuckets) if (b.reset < now) rateBuckets.delete(id);
}

function checkRateLimit(clientId: string): { ok: boolean; count: number } {
  const now = Date.now();
  pruneRateBuckets();
  let b = rateBuckets.get(clientId);
  if (!b || b.reset < now) {
    b = { count: 0, reset: now + RATE_WINDOW_MS };
    rateBuckets.set(clientId, b);
  }
  b.count += 1;
  return { ok: b.count <= RATE_MAX_REQUESTS, count: b.count };
}

function newOverflowState(): OverflowState {
  return {
    active: false,
    buffered: 0,
    lastTrigger: 0,
    lastExitCheck: 0,
    totalRouted: 0,
    totalDrained: 0,
    totalDropped: 0,
    mode: "normal",
  };
}

function pruneRecentMsgs(peer: Peer): void {
  const cutoff = Date.now() - RATE_WINDOW_MS;
  while (peer.recentMsgTimes.length > 0 && peer.recentMsgTimes[0] < cutoff) {
    peer.recentMsgTimes.shift();
  }
  if (peer.recentMsgTimes.length > 256) {
    peer.recentMsgTimes.splice(0, peer.recentMsgTimes.length - 256);
  }
}

function computeCurrentRate(peer: Peer): number {
  pruneRecentMsgs(peer);
  return peer.recentMsgTimes.length;
}

function updateOverflowState(peer: Peer): void {
  const rate = computeCurrentRate(peer);
  const now = Date.now();
  const of = peer.overflow;

  if (rate >= RATE_OVERFLOW_TRIGGER) {
    if (!of.active) {
      of.active = true;
      of.lastTrigger = now;
      of.mode = rate >= RATE_OVERFLOW_TRIGGER * 2 ? "critical" : "overflow";
    }
    of.mode = rate >= RATE_OVERFLOW_TRIGGER * 2 ? "critical" : "overflow";
    of.lastExitCheck = now;
    return;
  }

  if (of.active && rate <= RATE_OVERFLOW_EXIT) {
    if (of.lastExitCheck === 0) of.lastExitCheck = now;
    if (now - of.lastExitCheck >= OVERFLOW_EXIT_HOLD_MS) {
      of.active = false;
      of.mode = "normal";
      of.lastExitCheck = 0;
    }
  } else if (of.active && rate > RATE_OVERFLOW_EXIT) {
    of.lastExitCheck = now;
    of.mode = rate > RATE_OVERFLOW_TRIGGER ? "overflow" : "warn";
  }
}

function getRoom(id: string): Room {
  let r = rooms.get(id);
  if (!r) {
    r = {
      peers: new Map(),
      sockets: new Map(),
      host: null,
      banned: new Set(),
      bannedNames: new Set(),
      openControl: true,
      roomSecret: null,
      createdAt: Date.now(),
      controlLog: [],
      capabilityIndex: new Map(),
      doorCache: new Map(),
      floorType: "unknown",
      maxDoorSeen: 0,
      overflowLog: [],
      gameOverflow: null,
    };
    rooms.set(id, r);
  }
  return r;
}

function newSessionToken(): string {
  const buf = new Uint8Array(24);
  crypto.getRandomValues(buf);
  return Array.from(buf).map((b) => b.toString(16).padStart(2, "0")).join("");
}

const KEY_CACHE = new Map<string, CryptoKey>();

async function hmacKey(secret: string): Promise<CryptoKey> {
  const cached = KEY_CACHE.get(secret);
  if (cached) return cached;
  const enc = new TextEncoder().encode(secret);
  const key = await crypto.subtle.importKey(
    "raw", enc, { name: "HMAC", hash: "SHA-256" }, false, ["sign", "verify"],
  );
  KEY_CACHE.set(secret, key);
  if (KEY_CACHE.size > 512) KEY_CACHE.delete(KEY_CACHE.keys().next().value!);
  return key;
}

async function signPayload(secret: string, payload: string): Promise<string> {
  const key = await hmacKey(secret);
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(payload));
  return Array.from(new Uint8Array(sig)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function verifySignature(secret: string, payload: string, sig: string): Promise<boolean> {
  if (!secret || !sig) return true;
  try {
    const expected = await signPayload(secret, payload);
    if (expected.length !== sig.length) return false;
    let diff = 0;
    for (let i = 0; i < expected.length; i++) {
      diff |= expected.charCodeAt(i) ^ sig.charCodeAt(i);
    }
    return diff === 0;
  } catch {
    return false;
  }
}

async function loadBanList(roomId: string): Promise<void> {
  const room = getRoom(roomId);
  const iter = kv.list<string>({ prefix: ["ban", roomId] });
  for await (const e of iter) {
    const id = String(e.key[e.key.length - 1]);
    room.banned.add(id);
  }
}

async function persistBan(roomId: string, peerId: string, name: string): Promise<void> {
  await kv.set(["ban", roomId, peerId], name, { expireIn: BAN_TTL_MS });
}

async function persistUnban(roomId: string, peerId: string): Promise<void> {
  await kv.delete(["ban", roomId, peerId]);
}

async function nextMessageId(room: string): Promise<number> {
  const key: Deno.KvKey = ["counter", room];
  for (let i = 0; i < 10; i++) {
    const current = await kv.get<bigint>(key);
    const next = (current.value ?? 0n) + 1n;
    const res = await kv.atomic().check(current).set(key, next).commit();
    if (res.ok) return Number(next);
  }
  throw new Error("counter_fail");
}

async function persist(room: string, id: number, envelope: unknown, ttl: number) {
  await kv.set(["msg", room, id], envelope, { expireIn: ttl });
}

async function persistDoor(roomId: string, door: DoorState): Promise<void> {
  await kv.set(
    ["door", roomId, door.sector, door.index],
    door,
    { expireIn: DOOR_TTL_MS },
  );
}

async function loadDoors(roomId: string): Promise<void> {
  const room = getRoom(roomId);
  const iter = kv.list<DoorState>({ prefix: ["door", roomId] });
  for await (const e of iter) {
    const d = e.value;
    if (d && typeof d.index === "number" && typeof d.sector === "string") {
      room.doorCache.set(doorKey(d.sector, d.index), d);
    }
  }
}

async function persistOverflow(roomId: string, peerId: string, id: number, envelope: unknown): Promise<void> {
  await kv.set(
    ["overflow", roomId, peerId, id],
    envelope,
    { expireIn: OVERFLOW_TTL_MS },
  );
}

async function drainOverflow(roomId: string, peerId: string, limit = OVERFLOW_DRAIN_BATCH): Promise<unknown[]> {
  const out: unknown[] = [];
  const toDelete: Deno.KvKey[] = [];

  const iter = kv.list<unknown>(
    { prefix: ["overflow", roomId, peerId] },
    { limit },
  );

  for await (const e of iter) {
    out.push(e.value);
    toDelete.push(e.key);
  }

  await Promise.all(toDelete.map((k) => kv.delete(k)));
  return out;
}

async function countOverflow(roomId: string, peerId: string): Promise<number> {
  let count = 0;
  const iter = kv.list({ prefix: ["overflow", roomId, peerId] }, { limit: OVERFLOW_BUFFER_MAX + 1 });
  for await (const _ of iter) count++;
  return count;
}

async function clearOverflow(roomId: string, peerId: string): Promise<number> {
  const keys: Deno.KvKey[] = [];
  const iter = kv.list({ prefix: ["overflow", roomId, peerId] }, { limit: OVERFLOW_BUFFER_MAX + 1 });
  for await (const e of iter) keys.push(e.key);
  await Promise.all(keys.map((k) => kv.delete(k)));
  return keys.length;
}

function routeThroughOverflow(
  _roomId: string,
  peer: Peer,
  _envelope: unknown,
): { buffered: boolean } {
  const of = peer.overflow;
  if (of.buffered >= OVERFLOW_BUFFER_MAX) {
    of.totalDropped++;
    return { buffered: false };
  }
  of.buffered++;
  of.totalRouted++;
  return { buffered: true };
}

function broadcastToRoom(
  roomId: string,
  payload: unknown,
  exceptPeerId?: string,
  channelFilter?: string,
): number {
  const room = rooms.get(roomId);
  if (!room) return 0;
  const text = JSON.stringify(payload);
  const ch = (payload as { ch?: string })?.ch;
  let sent = 0;
  for (const [pid, ws] of room.sockets) {
    if (pid === exceptPeerId) continue;
    if (channelFilter && ch && ch !== channelFilter) continue;
    try {
      if (ws.readyState === WebSocket.OPEN) { ws.send(text); sent++; }
    } catch { /* ignore */ }
  }
  return sent;
}

function sendToPeer(roomId: string, targetId: string, payload: unknown): boolean {
  const room = rooms.get(roomId);
  if (!room) return false;
  const ws = room.sockets.get(targetId);
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try { ws.send(JSON.stringify(payload)); return true; } catch { return false; }
}

function logControl(
  room: Room,
  from: string,
  cmd: string,
  to: string,
  ok: boolean,
  reason?: string,
): void {
  room.controlLog.push({ ts: Date.now(), from, cmd, to, ok, reason });
  if (room.controlLog.length > CONTROL_LOG_MAX) {
    room.controlLog.splice(0, room.controlLog.length - CONTROL_LOG_MAX);
  }
}

function logOverflow(room: Room, peer: string, buffered: number, drained: number): void {
  room.overflowLog.push({ ts: Date.now(), peer, buffered, drained });
  if (room.overflowLog.length > 100) {
    room.overflowLog.splice(0, room.overflowLog.length - 100);
  }
}

function canIssue(room: Room, issuer: Peer, cmd: string): boolean {
  if (room.openControl) return true;
  if (HOST_ONLY.has(cmd)) return issuer.isHost;
  return issuer.isHost;
}

function registerCapability(room: Room, peer: Peer, cap: PeerCapability): void {
  if (peer.capabilities.size >= MAX_CAPS_PER_PEER) return;
  peer.capabilities.set(cap.name, cap);
  let set = room.capabilityIndex.get(cap.name);
  if (!set) { set = new Set(); room.capabilityIndex.set(cap.name, set); }
  set.add(peer.id);
}

function unregisterCapabilities(room: Room, peerId: string): void {
  const peer = room.peers.get(peerId);
  if (!peer) return;
  for (const name of peer.capabilities.keys()) {
    const set = room.capabilityIndex.get(name);
    if (set) {
      set.delete(peerId);
      if (set.size === 0) room.capabilityIndex.delete(name);
    }
  }
  peer.capabilities.clear();
}

function kickPeer(
  roomId: string,
  targetId: string,
  reason: string,
): { ok: boolean; name: string } {
  const room = rooms.get(roomId);
  if (!room) return { ok: false, name: "" };

  const target = room.peers.get(targetId);
  const name = target?.name ?? "unknown";

  const ws = room.sockets.get(targetId);
  if (ws) {
    try {
      ws.send(JSON.stringify({
        t: "ctrl", cmd: "@KICK", reason, ts: Date.now(),
      }));
      ws.close(1000, "kicked");
    } catch { /* ignore */ }
  }

  room.sockets.delete(targetId);
  room.peers.delete(targetId);
  unregisterCapabilities(room, targetId);
  room.banned.add(targetId);
  room.bannedNames.add(name);

  if (room.host === targetId) {
    const remaining = [...room.peers.values()];
    room.host = remaining.length > 0 ? remaining[0].id : null;
    if (room.host) {
      const newHost = room.peers.get(room.host);
      if (newHost) newHost.isHost = true;
    }
  }

  return { ok: true, name };
}

function newChunkBuffer(
  id: string,
  cmd: string,
  target: string,
  channel: string,
  total: number,
  meta: Record<string, unknown>,
): ChunkBuffer {
  return {
    id, cmd, target, channel, total, received: 0,
    pieces: new Map(),
    startedAt: Date.now(),
    lastAt: Date.now(),
    meta,
  };
}

function cleanupChunks(peer: Peer): void {
  const now = Date.now();
  for (const [id, buf] of peer.chunks) {
    if (now - buf.lastAt > CHUNK_TIMEOUT_MS) peer.chunks.delete(id);
  }
}

async function handleChunk(
  roomId: string,
  peer: Peer,
  cmd: string,
  payload: Record<string, unknown>,
  socket: WebSocket,
): Promise<void> {
  cleanupChunks(peer);
  const now = Date.now();

  const chunkId = isString(payload.chunkId, 128) ? payload.chunkId as string : null;
  if (!chunkId) {
    socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_chunk_id" }));
    return;
  }

  if (cmd === "@CHUNK_BEGIN") {
    const total = isNumber(payload.total) ? Math.floor(payload.total as number) : 0;
    if (total <= 0 || total > MAX_CHUNKS) {
      socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_chunk_total" }));
      return;
    }
    const innerCmd = isString(payload.c, 128) ? payload.c as string : "MSG";
    const target = isString(payload.to, 64) ? payload.to as string : "*";
    const channel = sanitizeChannel(payload.ch);
    const meta = (typeof payload.meta === "object" && payload.meta !== null)
      ? payload.meta as Record<string, unknown>
      : {};

    const buf = newChunkBuffer(chunkId, innerCmd, target, channel, total, meta);
    peer.chunks.set(chunkId, buf);

    socket.send(JSON.stringify({
      t: "sys", event: "chunk_started", chunkId, total, ts: now,
    }));
    return;
  }

  if (cmd === "@CHUNK") {
    const buf = peer.chunks.get(chunkId);
    if (!buf) {
      socket.send(JSON.stringify({ t: "sys", event: "error", error: "no_chunk_begin" }));
      return;
    }
    const idx = isNumber(payload.idx) ? Math.floor(payload.idx as number) : -1;
    if (idx < 0 || idx >= buf.total) {
      socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_chunk_idx" }));
      return;
    }
    const piece = isString(payload.piece, MAX_BODY_OVERFLOW_BYTES) ? payload.piece as string : null;
    if (!piece) {
      socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_chunk_piece" }));
      return;
    }
    if (!buf.pieces.has(idx)) {
      buf.pieces.set(idx, piece);
      buf.received++;
    }
    buf.lastAt = now;

    socket.send(JSON.stringify({
      t: "sys", event: "chunk_ack", chunkId, idx, received: buf.received, total: buf.total,
    }));
    return;
  }

  if (cmd === "@CHUNK_END") {
    const buf = peer.chunks.get(chunkId);
    if (!buf) {
      socket.send(JSON.stringify({ t: "sys", event: "error", error: "no_chunk_begin" }));
      return;
    }
    if (buf.received < buf.total) {
      socket.send(JSON.stringify({
        t: "sys", event: "error", error: "chunk_incomplete",
        received: buf.received, total: buf.total,
      }));
      return;
    }

    const pieces: string[] = [];
    for (let i = 0; i < buf.total; i++) {
      const p = buf.pieces.get(i);
      if (!p) {
        socket.send(JSON.stringify({
          t: "sys", event: "error", error: "chunk_gap", missing: i,
        }));
        peer.chunks.delete(chunkId);
        return;
      }
      pieces.push(p);
    }

    const reassembled = pieces.join("");
    peer.chunks.delete(chunkId);

    let data: unknown = reassembled;
    try {
      data = JSON.parse(reassembled);
    } catch {
      data = reassembled;
    }

    const room = rooms.get(roomId);
    if (!room) return;

    if (buf.cmd.startsWith("@")) {
      if (DOOR_CMDS.has(buf.cmd)) {
        const doorPayload = (typeof data === "object" && data !== null)
          ? data as Record<string, unknown>
          : { sector: "A", index: 0 };
        await applyDoor(roomId, peer, buf.cmd, doorPayload);
      } else if (CONTROL_CMDS.has(buf.cmd)) {
        await applyControl(roomId, peer.id, peer.name, buf.cmd, buf.target, String(data));
      } else if (RPC_CMDS.has(buf.cmd)) {
        await handleRpc(roomId, peer, buf.cmd, buf.target, data, now);
      }
      socket.send(JSON.stringify({
        t: "sys", event: "chunk_complete", chunkId, cmd: buf.cmd, size: reassembled.length,
      }));
      return;
    }

    if (peer.muted) {
      socket.send(JSON.stringify({ t: "sys", event: "error", error: "muted" }));
      return;
    }

    const id = await nextMessageId(roomId);
    const envelope = {
      t: "msg", id, ch: buf.channel,
      f: peer.id, n: peer.name,
      to: buf.target, c: buf.cmd, d: data, ts: now,
      meta: { ...buf.meta, chunked: true, chunks: buf.total },
    };

    await persist(roomId, id, envelope, MSG_TTL_MS);
    if (buf.target === "*") broadcastToRoom(roomId, envelope, peer.id);
    else sendToPeer(roomId, buf.target, envelope);

    socket.send(JSON.stringify({
      t: "sys", event: "chunk_complete", chunkId, cmd: buf.cmd,
      size: reassembled.length, msgId: id,
    }));
    return;
  }

  if (cmd === "@CHUNK_ABORT") {
    const existed = peer.chunks.delete(chunkId);
    socket.send(JSON.stringify({
      t: "sys", event: "chunk_aborted", chunkId, existed,
    }));
  }
}

async function handleOverflow(
  roomId: string,
  peer: Peer,
  cmd: string,
  payload: Record<string, unknown>,
): Promise<Response> {
  const room = getRoom(roomId);
  const now = Date.now();

  switch (cmd) {
    case "@OVERFLOW_STATUS": {
      const buffered = await countOverflow(roomId, peer.id);
      peer.overflow.buffered = buffered;
      return jsonResponse({
        ok: true,
        peerId: peer.id,
        active: peer.overflow.active,
        mode: peer.overflow.mode,
        buffered,
        capacity: OVERFLOW_BUFFER_MAX,
        rate: computeCurrentRate(peer),
        threshold: RATE_OVERFLOW_TRIGGER,
        exitThreshold: RATE_OVERFLOW_EXIT,
        totalRouted: peer.overflow.totalRouted,
        totalDrained: peer.overflow.totalDrained,
        totalDropped: peer.overflow.totalDropped,
        ts: now,
      });
    }

    case "@OVERFLOW_DRAIN": {
      const limitRaw = isNumber(payload.limit) ? Math.floor(payload.limit as number) : OVERFLOW_DRAIN_BATCH;
      const limit = Math.max(1, Math.min(limitRaw, OVERFLOW_DRAIN_BATCH * 2));
      const drained = await drainOverflow(roomId, peer.id, limit);
      peer.overflow.buffered = Math.max(0, peer.overflow.buffered - drained.length);
      peer.overflow.totalDrained += drained.length;

      logOverflow(room, peer.id, drained.length, drained.length);

      return jsonResponse({
        ok: true,
        drained: drained.length,
        messages: drained,
        remaining: await countOverflow(roomId, peer.id),
        ts: now,
      });
    }

    case "@OVERFLOW_CLEAR": {
      const cleared = await clearOverflow(roomId, peer.id);
      peer.overflow.buffered = 0;
      logOverflow(room, peer.id, cleared, 0);
      return jsonResponse({ ok: true, cleared, ts: now });
    }

    case "@OVERFLOW_RESUME": {
      const since = isNumber(payload.since) ? Math.floor(payload.since as number) : 0;
      const drained = await drainOverflow(roomId, peer.id, OVERFLOW_DRAIN_BATCH * 2);
      const filtered = drained.filter((m) => {
        const id = (m as { id?: number })?.id;
        return typeof id === "number" ? id > since : true;
      });
      return jsonResponse({
        ok: true,
        resumed: filtered.length,
        messages: filtered,
        remaining: await countOverflow(roomId, peer.id),
        ts: now,
      });
    }

    case "@OVERFLOW_STATS": {
      return jsonResponse({
        ok: true,
        peer: {
          active: peer.overflow.active,
          mode: peer.overflow.mode,
          rate: computeCurrentRate(peer),
          buffered: await countOverflow(roomId, peer.id),
          totalRouted: peer.overflow.totalRouted,
          totalDrained: peer.overflow.totalDrained,
          totalDropped: peer.overflow.totalDropped,
        },
        room: {
          recentOverflows: room.overflowLog.slice(-20),
          peersInOverflow: [...room.peers.values()].filter((p) => p.overflow.active).length,
        },
        config: {
          trigger: RATE_OVERFLOW_TRIGGER,
          exit: RATE_OVERFLOW_EXIT,
          bufferMax: OVERFLOW_BUFFER_MAX,
          chunkSize: CHUNK_SIZE,
          maxChunks: MAX_CHUNKS,
        },
        ts: now,
      });
    }

    default:
      return jsonResponse({ error: "unknown_overflow_cmd" }, 400);
  }
}

function fireGameOverflow(roomId: string, sessionId: string): void {
  const room = rooms.get(roomId);
  if (!room?.gameOverflow || room.gameOverflow.id !== sessionId) return;
  const session = room.gameOverflow;
  if (session.active) return;

  const now = Date.now();
  session.active = true;
  session.startedAt = now;
  session.stopAt = now + session.durationMs;
  session.syncEpoch = now + GAMEFLOW_SYNC_LEAD_MS;

  broadcastToRoom(roomId, {
    t: "game_overflow",
    cmd: "@GAME_OVERFLOW_GO",
    sessionId,
    mode: session.mode,
    target: session.target,
    intensity: session.intensity,
    durationMs: session.durationMs,
    syncEpoch: session.syncEpoch,
    stopAt: session.stopAt,
    participants: [...session.participants],
    ts: now,
  });

  setTimeout(() => {
    const cur = rooms.get(roomId);
    if (!cur?.gameOverflow || cur.gameOverflow.id !== sessionId) return;
    if (!cur.gameOverflow.active) return;
    const s = cur.gameOverflow;
    s.active = false;
    broadcastToRoom(roomId, {
      t: "game_overflow",
      cmd: "@GAME_OVERFLOW_AUTO_STOP",
      sessionId,
      reason: "duration_elapsed",
      stats: {
        totalTicks: s.stats.totalTicks,
        perPeer: Object.fromEntries(s.stats.perPeer),
        peakRate: s.stats.peakRate,
      },
      ts: Date.now(),
    });
    cur.gameOverflow = null;
  }, session.durationMs + GAMEFLOW_SYNC_LEAD_MS + 100);
}

async function handleGameOverflow(
  roomId: string,
  peer: Peer,
  cmd: string,
  payload: Record<string, unknown>,
): Promise<Response> {
  const room = getRoom(roomId);
  const now = Date.now();

  switch (cmd) {
    case "@GAME_OVERFLOW_BEGIN": {
      if (room.gameOverflow?.active) {
        return jsonResponse(
          { error: "already_active", sessionId: room.gameOverflow.id },
          409,
        );
      }
      if (room.gameOverflow && !room.gameOverflow.active) {
        room.gameOverflow = null;
      }

      if (!canIssue(room, peer, "@GAME_OVERFLOW_BEGIN")) {
        return jsonResponse({ error: "no_permission" }, 403);
      }

      const rawMode = String(payload.mode ?? "burst");
      const mode = (["burst", "sustain", "pulse", "wave"].includes(rawMode)
        ? rawMode
        : "burst") as GameOverflowMode;

      const target = sanitizeChannel(payload.target ?? "chat");

      const durationMs = Math.min(
        GAMEFLOW_MAX_DURATION_MS,
        Math.max(
          GAMEFLOW_MIN_DURATION_MS,
          isNumber(payload.durationMs) ? (payload.durationMs as number) : 5_000,
        ),
      );

      const intensity = Math.min(
        GAMEFLOW_MAX_INTENSITY,
        Math.max(
          GAMEFLOW_MIN_INTENSITY,
          isNumber(payload.intensity) ? Math.floor(payload.intensity as number) : 5,
        ),
      );

      const sessionId = `go_${now}_${Math.random().toString(36).slice(2, 8)}`;

      const session: GameOverflowSession = {
        id: sessionId,
        mode,
        target,
        initiatedBy: peer.id,
        participants: new Set([peer.id]),
        ready: new Set([peer.id]),
        startedAt: now,
        stopAt: now + durationMs + GAMEFLOW_READY_TIMEOUT + GAMEFLOW_SYNC_LEAD_MS,
        durationMs,
        intensity,
        active: false,
        syncEpoch: 0,
        stats: {
          totalTicks: 0,
          perPeer: new Map([[peer.id, 0]]),
          peakRate: 0,
        },
      };

      room.gameOverflow = session;

      broadcastToRoom(roomId, {
        t: "game_overflow",
        cmd: "@GAME_OVERFLOW_INVITE",
        sessionId,
        mode,
        target,
        intensity,
        durationMs,
        from: peer.id,
        fromName: peer.name,
        readyTimeout: GAMEFLOW_READY_TIMEOUT,
        ts: now,
      }, peer.id);

      setTimeout(() => {
        const cur = rooms.get(roomId);
        if (!cur?.gameOverflow || cur.gameOverflow.id !== sessionId) return;
        if (cur.gameOverflow.active) return;
        fireGameOverflow(roomId, sessionId);
      }, GAMEFLOW_READY_TIMEOUT);

      return jsonResponse({
        ok: true,
        sessionId,
        mode,
        target,
        intensity,
        durationMs,
        waiting: [...room.peers.keys()].filter((p) => !session.ready.has(p)),
        ts: now,
      });
    }

    case "@GAME_OVERFLOW_READY": {
      const session = room.gameOverflow;
      if (!session || session.active) {
        return jsonResponse({ error: "no_session" }, 404);
      }

      session.ready.add(peer.id);
      session.participants.add(peer.id);
      if (!session.stats.perPeer.has(peer.id)) {
        session.stats.perPeer.set(peer.id, 0);
      }

      broadcastToRoom(roomId, {
        t: "game_overflow",
        cmd: "@GAME_OVERFLOW_PEER_READY",
        sessionId: session.id,
        peer: peer.id,
        peerName: peer.name,
        readyCount: session.ready.size,
        totalPeers: room.peers.size,
        ts: now,
      }, peer.id);

      const allReady = [...room.peers.keys()].every((p) => session.ready.has(p));
      if (allReady) fireGameOverflow(roomId, session.id);

      return jsonResponse({ ok: true, readyCount: session.ready.size, allReady });
    }

    case "@GAME_OVERFLOW_STOP": {
      const session = room.gameOverflow;
      if (!session) return jsonResponse({ error: "no_session" }, 404);

      const isInitiator = session.initiatedBy === peer.id;
      const isHost = peer.isHost;
      const openCtrl = room.openControl;

      if (!isInitiator && !isHost && !openCtrl) {
        return jsonResponse({ error: "no_permission" }, 403);
      }

      session.active = false;
      const finalStats = {
        sessionId: session.id,
        durationMs: now - session.startedAt,
        totalTicks: session.stats.totalTicks,
        perPeer: Object.fromEntries(session.stats.perPeer),
        peakRate: session.stats.peakRate,
      };

      broadcastToRoom(roomId, {
        t: "game_overflow",
        cmd: "@GAME_OVERFLOW_STOPPED",
        sessionId: session.id,
        stoppedBy: peer.id,
        reason: String(payload.reason ?? "manual"),
        stats: finalStats,
        ts: now,
      });

      room.gameOverflow = null;
      return jsonResponse({ ok: true, ...finalStats });
    }

    case "@GAME_OVERFLOW_STATUS": {
      const session = room.gameOverflow;
      if (!session) {
        return jsonResponse({
          ok: true,
          active: false,
          session: null,
          config: {
            maxDurationMs: GAMEFLOW_MAX_DURATION_MS,
            maxIntensity: GAMEFLOW_MAX_INTENSITY,
            syncLead: GAMEFLOW_SYNC_LEAD_MS,
            readyTimeout: GAMEFLOW_READY_TIMEOUT,
          },
        });
      }

      const elapsed = now - session.startedAt;

      return jsonResponse({
        ok: true,
        active: session.active,
        session: {
          id: session.id,
          mode: session.mode,
          target: session.target,
          initiatedBy: session.initiatedBy,
          intensity: session.intensity,
          durationMs: session.durationMs,
          startedAt: session.startedAt,
          stopAt: session.stopAt,
          syncEpoch: session.syncEpoch,
          elapsed,
          remaining: Math.max(0, session.stopAt - now),
          participants: [...session.participants],
          ready: [...session.ready],
          waiting: [...room.peers.keys()].filter((p) => !session.ready.has(p)),
          stats: {
            totalTicks: session.stats.totalTicks,
            perPeer: Object.fromEntries(session.stats.perPeer),
            peakRate: session.stats.peakRate,
          },
        },
        ts: now,
      });
    }

    case "@GAME_OVERFLOW_TICK": {
      const session = room.gameOverflow;
      if (!session || !session.active) {
        return jsonResponse({ ok: false, error: "no_active_session" }, 404);
      }

      const count = Math.min(
        100,
        Math.max(1, isNumber(payload.count) ? Math.floor(payload.count as number) : 1),
      );

      const cur = session.stats.perPeer.get(peer.id) ?? 0;
      session.stats.perPeer.set(peer.id, cur + count);
      session.stats.totalTicks += count;

      const elapsedSec = Math.max(0.001, (now - session.startedAt) / 1000);
      const rate = session.stats.totalTicks / elapsedSec;
      if (rate > session.stats.peakRate) session.stats.peakRate = rate;

      return jsonResponse({
        ok: true,
        total: session.stats.totalTicks,
        rate,
        remaining: Math.max(0, session.stopAt - now),
      });
    }

    case "@GAME_OVERFLOW_EXTEND": {
      const session = room.gameOverflow;
      if (!session) return jsonResponse({ error: "no_session" }, 404);

      if (!canIssue(room, peer, "@GAME_OVERFLOW_EXTEND")) {
        return jsonResponse({ error: "no_permission" }, 403);
      }

      const extra = Math.min(
        GAMEFLOW_MAX_DURATION_MS,
        Math.max(0, isNumber(payload.extraMs) ? (payload.extraMs as number) : 0),
      );

      session.stopAt = Math.min(
        session.stopAt + extra,
        now + GAMEFLOW_MAX_DURATION_MS,
      );

      broadcastToRoom(roomId, {
        t: "game_overflow",
        cmd: "@GAME_OVERFLOW_EXTENDED",
        sessionId: session.id,
        extraMs: extra,
        newStopAt: session.stopAt,
        ts: now,
      });

      return jsonResponse({ ok: true, newStopAt: session.stopAt, extraMs: extra });
    }

    case "@GAME_OVERFLOW_KICK_PEER": {
      const session = room.gameOverflow;
      if (!session || !session.active) {
        return jsonResponse({ error: "no_active_session" }, 404);
      }

      const targetPeer = isString(payload.peer, 64) ? (payload.peer as string) : null;
      if (!targetPeer) return jsonResponse({ error: "bad_peer" }, 400);
      if (targetPeer === session.initiatedBy) {
        return jsonResponse({ error: "cannot_remove_initiator" }, 400);
      }

      session.participants.delete(targetPeer);
      session.ready.delete(targetPeer);
      session.stats.perPeer.delete(targetPeer);

      broadcastToRoom(roomId, {
        t: "game_overflow",
        cmd: "@GAME_OVERFLOW_PEER_REMOVED",
        sessionId: session.id,
        peer: targetPeer,
        by: peer.id,
        ts: now,
      });

      return jsonResponse({ ok: true, participants: [...session.participants] });
    }

    default:
      return jsonResponse({ error: "unknown_game_overflow_cmd" }, 400);
  }
}

async function applyDoor(
  roomId: string,
  fromPeer: Peer,
  cmd: string,
  payload: Record<string, unknown>,
): Promise<Response> {
  const room = getRoom(roomId);
  const now = Date.now();

  const sector = sanitizeSector(String(payload.sector ?? "A"));
  if (!sector || !isValidSector(sector)) {
    return jsonResponse({ error: "invalid_sector" }, 400);
  }

  const index = Math.max(0, Math.min(ARCHIVES_DOORS_PER_SEC, Number(payload.index ?? 0)));
  if (!isNumber(index) || index < 0 || index > ARCHIVES_DOORS_PER_SEC) {
    return jsonResponse({ error: "invalid_index" }, 400);
  }

  const key = doorKey(sector, index);

  switch (cmd) {
    case "@DOOR_OPEN": {
      const door: DoorState = {
        sector, index,
        opened: true, locked: false,
        openedBy: fromPeer.id,
        openedAt: now,
        updatedAt: now,
      };
      room.doorCache.set(key, door);
      await persistDoor(roomId, door);

      const orderIndex = (sector.charCodeAt(0) - 65) * ARCHIVES_DOORS_PER_SEC + index;
      if (orderIndex > room.maxDoorSeen) room.maxDoorSeen = orderIndex;

      broadcastToRoom(roomId, {
        t: "door", cmd: "@DOOR_OPEN",
        from: fromPeer.id, sector, index,
        totalSeen: room.maxDoorSeen,
        ts: now,
      }, fromPeer.id);

      return jsonResponse({ ok: true, door, totalSeen: room.maxDoorSeen });
    }

    case "@DOOR_CLOSE": {
      const door: DoorState = {
        sector, index,
        opened: false, locked: false,
        updatedAt: now,
      };
      room.doorCache.set(key, door);
      await persistDoor(roomId, door);

      broadcastToRoom(roomId, {
        t: "door", cmd: "@DOOR_CLOSE",
        from: fromPeer.id, sector, index, ts: now,
      }, fromPeer.id);

      return jsonResponse({ ok: true, door });
    }

    case "@DOOR_LOCK": {
      const door = room.doorCache.get(key) ?? {
        sector, index,
        opened: false, locked: false, updatedAt: now,
      };
      door.locked = true;
      door.updatedAt = now;
      room.doorCache.set(key, door);
      await persistDoor(roomId, door);

      broadcastToRoom(roomId, {
        t: "door", cmd: "@DOOR_LOCK",
        from: fromPeer.id, sector, index, ts: now,
      }, fromPeer.id);

      return jsonResponse({ ok: true, door });
    }

    case "@DOOR_RESET": {
      const prefix = payload.sector ? sector : null;
      const toDelete: Deno.KvKey[] = [];
      for await (const e of kv.list({ prefix: ["door", roomId] })) {
        if (!prefix || e.key[2] === prefix) toDelete.push(e.key);
      }
      await Promise.all(toDelete.map((k) => kv.delete(k)));

      if (prefix) {
        for (const [k] of room.doorCache) {
          if (k.startsWith(prefix + "-")) room.doorCache.delete(k);
        }
      } else {
        room.doorCache.clear();
        room.maxDoorSeen = 0;
      }

      broadcastToRoom(roomId, {
        t: "door", cmd: "@DOOR_RESET",
        from: fromPeer.id, sector: prefix, ts: now,
      });

      return jsonResponse({ ok: true, cleared: toDelete.length });
    }

    case "@DOOR_SYNC": {
      const batchRaw = payload.batch;
      if (!Array.isArray(batchRaw)) {
        return jsonResponse({ error: "batch_not_array" }, 400);
      }
      if (batchRaw.length > MAX_DOOR_BATCH) {
        return jsonResponse({ error: "batch_too_large", max: MAX_DOOR_BATCH }, 413);
      }

      const applied: string[] = [];
      const writes: Promise<void>[] = [];

      for (const raw of batchRaw) {
        if (typeof raw !== "object" || raw === null) continue;
        const d = raw as Record<string, unknown>;
        const s = sanitizeSector(String(d.sector ?? ""));
        const i = Number(d.index ?? -1);
        if (!s || !isValidSector(s) || i < 0 || i > ARCHIVES_DOORS_PER_SEC) continue;

        const state: DoorState = {
          sector: s,
          index: i,
          opened: Boolean(d.opened),
          locked: Boolean(d.locked),
          openedBy: typeof d.openedBy === "string" ? d.openedBy : undefined,
          openedAt: isNumber(d.openedAt) ? d.openedAt : undefined,
          updatedAt: now,
        };
        const k = doorKey(s, i);
        room.doorCache.set(k, state);
        writes.push(persistDoor(roomId, state));
        applied.push(k);

        const orderIndex = (s.charCodeAt(0) - 65) * ARCHIVES_DOORS_PER_SEC + i;
        if (orderIndex > room.maxDoorSeen) room.maxDoorSeen = orderIndex;
      }

      await Promise.all(writes);

      broadcastToRoom(roomId, {
        t: "door", cmd: "@DOOR_SYNC",
        from: fromPeer.id,
        count: applied.length,
        keys: applied,
        totalSeen: room.maxDoorSeen,
        ts: now,
      }, fromPeer.id);

      return jsonResponse({
        ok: true, applied: applied.length, totalSeen: room.maxDoorSeen,
      });
    }

    default:
      return jsonResponse({ error: "unknown_door_cmd" }, 400);
  }
}

async function applyControl(
  roomId: string,
  issuerId: string,
  _issuerName: string,
  cmd: string,
  targetId: string,
  payload: string,
): Promise<Response> {
  const room = getRoom(roomId);
  const issuer = room.peers.get(issuerId);

  if (!issuer) return jsonResponse({ error: "issuer_not_in_room" }, 403);
  if (issuer.muted && cmd !== "@UNMUTE") return jsonResponse({ error: "issuer_muted" }, 403);
  if (!canIssue(room, issuer, cmd)) {
    logControl(room, issuerId, cmd, targetId, false, "no_permission");
    return jsonResponse({ error: "no_permission" }, 403);
  }

  const now = Date.now();
  if (now - issuer.lastControlAt < CONTROL_RATE_MS) {
    return jsonResponse({ error: "control_rate_limited" }, 429);
  }
  issuer.lastControlAt = now;

  const result: Record<string, unknown> = { ok: true, cmd, target: targetId, ts: now };

  switch (cmd) {
    case "@KICK": {
      if (now - issuer.lastKickAt < KICK_COOLDOWN_MS) return jsonResponse({ error: "kick_cooldown" }, 429);
      issuer.lastKickAt = now;
      if (targetId === issuerId) return jsonResponse({ error: "cannot_kick_self" }, 400);

      const out = kickPeer(roomId, targetId, payload || "kicked");
      if (!out.ok) { logControl(room, issuerId, cmd, targetId, false, "kick_failed"); return jsonResponse({ error: "kick_failed" }, 500); }

      logControl(room, issuerId, cmd, targetId, true);
      result.targetName = out.name;
      broadcastToRoom(roomId, {
        t: "ctrl", cmd: "@KICK", from: issuerId, to: targetId,
        targetName: out.name, reason: payload || "kicked", ts: now,
      });
      break;
    }

    case "@BAN": {
      if (now - issuer.lastKickAt < KICK_COOLDOWN_MS) return jsonResponse({ error: "ban_cooldown" }, 429);
      issuer.lastKickAt = now;
      if (targetId === issuerId) return jsonResponse({ error: "cannot_ban_self" }, 400);

      const out = kickPeer(roomId, targetId, payload || "banned");
      await persistBan(roomId, targetId, out.name);

      logControl(room, issuerId, cmd, targetId, true);
      result.targetName = out.name;
      result.ttlMs = BAN_TTL_MS;
      broadcastToRoom(roomId, {
        t: "ctrl", cmd: "@BAN", from: issuerId, to: targetId,
        targetName: out.name, reason: payload || "banned",
        ttlMs: BAN_TTL_MS, ts: now,
      });
      break;
    }

    case "@UNBAN": {
      if (!room.banned.has(targetId) && !room.bannedNames.has(targetId)) {
        return jsonResponse({ error: "not_banned" }, 404);
      }
      room.banned.delete(targetId);
      room.bannedNames.delete(targetId);
      await persistUnban(roomId, targetId);
      logControl(room, issuerId, cmd, targetId, true);
      broadcastToRoom(roomId, { t: "ctrl", cmd: "@UNBAN", from: issuerId, to: targetId, ts: now });
      break;
    }

    case "@MUTE":
    case "@UNMUTE": {
      const target = room.peers.get(targetId);
      if (!target) return jsonResponse({ error: "target_not_found" }, 404);
      target.muted = cmd === "@MUTE";
      logControl(room, issuerId, cmd, targetId, true);
      result.targetName = target.name;
      broadcastToRoom(roomId, { t: "ctrl", cmd, from: issuerId, to: targetId, targetName: target.name, ts: now });
      sendToPeer(roomId, targetId, { t: "ctrl", cmd, from: issuerId, ts: now });
      break;
    }

    case "@FREEZE":
    case "@UNFREEZE": {
      const target = room.peers.get(targetId);
      if (!target) return jsonResponse({ error: "target_not_found" }, 404);
      target.frozen = cmd === "@FREEZE";
      logControl(room, issuerId, cmd, targetId, true);
      result.targetName = target.name;
      broadcastToRoom(roomId, { t: "ctrl", cmd, from: issuerId, to: targetId, targetName: target.name, ts: now });
      sendToPeer(roomId, targetId, { t: "ctrl", cmd, from: issuerId, ts: now });
      break;
    }

    case "@PROMOTE":
    case "@DEMOTE": {
      const target = room.peers.get(targetId);
      if (!target) return jsonResponse({ error: "target_not_found" }, 404);

      if (cmd === "@PROMOTE") {
        for (const p of room.peers.values()) p.isHost = false;
        target.isHost = true;
        room.host = targetId;
      } else {
        target.isHost = false;
        if (room.host === targetId) room.host = issuer.id;
        issuer.isHost = true;
      }

      logControl(room, issuerId, cmd, targetId, true);
      broadcastToRoom(roomId, {
        t: "ctrl", cmd, from: issuerId, to: targetId,
        targetName: target.name, host: room.host, ts: now,
      });
      break;
    }

    case "@TRANSFER": {
      const target = room.peers.get(targetId);
      if (!target) return jsonResponse({ error: "target_not_found" }, 404);
      for (const p of room.peers.values()) p.isHost = false;
      target.isHost = true;
      room.host = targetId;
      logControl(room, issuerId, cmd, targetId, true);
      broadcastToRoom(roomId, {
        t: "ctrl", cmd, from: issuerId, to: targetId,
        targetName: target.name, host: room.host, ts: now,
      });
      break;
    }

    case "@PING": {
      const target = room.peers.get(targetId);
      if (!target) return jsonResponse({ error: "target_not_found" }, 404);
      sendToPeer(roomId, targetId, { t: "ctrl", cmd: "@PING", from: issuerId, ts: now });
      logControl(room, issuerId, cmd, targetId, true);
      result.targetName = target.name;
      result.alive = now - target.lastSeen < 5000;
      break;
    }

    case "@RESET": {
      room.banned.clear();
      room.bannedNames.clear();
      room.controlLog.length = 0;
      room.doorCache.clear();
      room.maxDoorSeen = 0;
      room.overflowLog.length = 0;

      const keys: Deno.KvKey[] = [];
      for await (const e of kv.list({ prefix: ["ban", roomId] })) keys.push(e.key);
      for await (const e of kv.list({ prefix: ["door", roomId] })) keys.push(e.key);
      for await (const e of kv.list({ prefix: ["overflow", roomId] })) keys.push(e.key);
      await Promise.all(keys.map((k) => kv.delete(k)));

      logControl(room, issuerId, cmd, "", true);
      broadcastToRoom(roomId, { t: "ctrl", cmd: "@RESET", from: issuerId, ts: now });
      break;
    }

    case "@KILL": {
      const target = room.peers.get(targetId);
      if (!target) return jsonResponse({ error: "target_not_found" }, 404);
      logControl(room, issuerId, cmd, targetId, true);
      result.targetName = target.name;
      sendToPeer(roomId, targetId, { t: "ctrl", cmd: "@KILL", from: issuerId, ts: now });
      broadcastToRoom(roomId, {
        t: "ctrl", cmd: "@KILL", from: issuerId, to: targetId,
        targetName: target.name, ts: now,
      });
      break;
    }

    case "@BROADCAST": {
      logControl(room, issuerId, cmd, "*", true);
      broadcastToRoom(roomId, {
        t: "ctrl", cmd: "@BROADCAST", from: issuerId,
        reason: payload, ts: now,
      });
      break;
    }

    default:
      return jsonResponse({ error: "unknown_control" }, 400);
  }

  return jsonResponse(result);
}

async function handleRpc(
  roomId: string,
  fromPeer: Peer,
  cmd: string,
  target: string,
  payload: unknown,
  corr: number,
): Promise<Response> {
  const room = getRoom(roomId);
  const now = Date.now();

  if (cmd === "@CALL") {
    if (!isString(target) || target === "*") return jsonResponse({ error: "rpc_needs_target" }, 400);
    const targetPeer = room.peers.get(target);
    if (!targetPeer) return jsonResponse({ error: "target_not_found" }, 404);

    if (fromPeer.rpcPending.size >= MAX_RPC_PENDING) {
      return jsonResponse({ error: "rpc_queue_full" }, 429);
    }

    fromPeer.rpcPending.set(corr, { target, ts: now, cmd: String((payload as { c?: string })?.c ?? "?") });

    const rpcMap = rpcRegistry.get(roomId) ?? new Map<number, RpcCorrelation>();
    rpcMap.set(corr, { issuedBy: fromPeer.id, issuedTo: target, cmd: String((payload as { c?: string })?.c ?? "?"), ts: now });
    rpcRegistry.set(roomId, rpcMap);

    const delivered = sendToPeer(roomId, target, {
      t: "rpc", cmd: "@CALL", from: fromPeer.id, to: target,
      corr, ch: sanitizeChannel((payload as { ch?: unknown })?.ch),
      c: (payload as { c?: unknown })?.c,
      d: (payload as { d?: unknown })?.d,
      ts: now,
    });

    if (!delivered) {
      fromPeer.rpcPending.delete(corr);
      rpcMap.delete(corr);
      return jsonResponse({ error: "peer_offline" }, 503);
    }

    return jsonResponse({ ok: true, corr, queued: true });
  }

  if (cmd === "@RETURN" || cmd === "@ERROR") {
    const rpcMap = rpcRegistry.get(roomId);
    if (!rpcMap) return jsonResponse({ error: "no_rpc_registry" }, 404);

    const origin = rpcMap.get(corr);
    if (!origin || origin.issuedTo !== fromPeer.id) {
      return jsonResponse({ error: "unknown_corr" }, 404);
    }

    sendToPeer(roomId, origin.issuedBy, {
      t: "rpc", cmd, from: fromPeer.id, to: origin.issuedBy,
      corr, d: (payload as { d?: unknown })?.d,
      c: origin.cmd, ts: now,
    });

    rpcMap.delete(corr);
    const issuer = room.peers.get(origin.issuedBy);
    if (issuer) issuer.rpcPending.delete(corr);

    return jsonResponse({ ok: true, corr, routed: true });
  }

  return jsonResponse({ error: "unknown_rpc" }, 400);
}

function validateEnvelope(msg: unknown): { ok: true; data: Record<string, unknown> } | { ok: false; error: string } {
  if (typeof msg !== "object" || msg === null) return { ok: false, error: "not_object" };
  const m = msg as Record<string, unknown>;

  if (m.c !== undefined && !isString(m.c, 128)) return { ok: false, error: "bad_c" };
  if (m.to !== undefined && !isString(m.to, 64)) return { ok: false, error: "bad_to" };
  if (m.d !== undefined) {
    const dj = JSON.stringify(m.d);
    if (dj.length > MAX_BODY_OVERFLOW_BYTES) return { ok: false, error: "data_too_big" };
  }
  if (m.ttl !== undefined && !isNumber(m.ttl)) return { ok: false, error: "bad_ttl" };
  if (m.corr !== undefined && !isNumber(m.corr)) return { ok: false, error: "bad_corr" };

  return { ok: true, data: m };
}

function notifyOverflow(socket: WebSocket, of: OverflowState): void {
  try {
    socket.send(JSON.stringify({
      t: "overflow",
      severity: of.mode === "critical" ? "critical" : "warn",
      active: of.active,
      mode: of.mode,
      buffered: of.buffered,
      capacity: OVERFLOW_BUFFER_MAX,
      ts: Date.now(),
    }));
  } catch { /* ignore */ }
}

async function handleWebSocket(
  req: Request,
  roomId: string,
  clientId: string,
  peerName: string,
): Promise<Response> {
  const room = getRoom(roomId);
  await loadBanList(roomId);
  await loadDoors(roomId);

  if (room.banned.has(clientId)) return jsonResponse({ error: "banned" }, 403);

  const roomSecret = req.headers.get("X-Room-Secret");
  if (roomSecret) {
    if (room.roomSecret === null) {
      room.roomSecret = roomSecret;
    } else if (room.roomSecret !== roomSecret) {
      return jsonResponse({ error: "wrong_room_secret" }, 403);
    }
  }

  const { socket, response } = Deno.upgradeWebSocket(req);
  const sessionToken = newSessionToken();

  socket.onopen = async () => {
    const now = Date.now();
    const isHost = room.host === null;
    if (isHost) room.host = clientId;

    const peer: Peer = {
      id: clientId,
      name: peerName,
      joinedAt: now,
      lastSeen: now,
      isHost,
      muted: false,
      frozen: false,
      banned: false,
      lastKickAt: 0,
      lastControlAt: 0,
      sessionToken,
      capabilities: new Map(),
      rpcPending: new Map(),
      overflow: newOverflowState(),
      chunks: new Map(),
      recentMsgTimes: [],
    };

    const buffered = await countOverflow(roomId, clientId);
    peer.overflow.buffered = buffered;
    if (buffered > 0) {
      peer.overflow.active = true;
      peer.overflow.mode = buffered > OVERFLOW_BUFFER_MAX / 2 ? "critical" : "overflow";
    }

    room.peers.set(clientId, peer);
    room.sockets.set(clientId, socket);

    broadcastToRoom(roomId, {
      t: "presence", event: "join",
      peer: { id: clientId, name: peerName, isHost },
      host: room.host, ts: now,
    }, clientId);

    socket.send(JSON.stringify({
      t: "welcome",
      peerId: clientId,
      sessionToken,
      room: roomId,
      isHost,
      host: room.host,
      openControl: room.openControl,
      maxDoorSeen: room.maxDoorSeen,
      floorType: room.floorType,
      doorsInCache: room.doorCache.size,
      totalDoorsTarget: room.floorType === "archives" ? ARCHIVES_TOTAL_DOORS : ROOMS_TOTAL_DOORS,
      overflow: {
        active: peer.overflow.active,
        mode: peer.overflow.mode,
        buffered,
        capacity: OVERFLOW_BUFFER_MAX,
        trigger: RATE_OVERFLOW_TRIGGER,
        exit: RATE_OVERFLOW_EXIT,
      },
      features: {
        overflow: true,
        chunks: true,
        chunkSize: CHUNK_SIZE,
        maxChunks: MAX_CHUNKS,
        backpressure: true,
        gameOverflow: true,
      },
      gameOverflowConfig: {
        maxDurationMs: GAMEFLOW_MAX_DURATION_MS,
        maxIntensity: GAMEFLOW_MAX_INTENSITY,
        syncLeadMs: GAMEFLOW_SYNC_LEAD_MS,
        readyTimeoutMs: GAMEFLOW_READY_TIMEOUT,
        modes: ["burst", "sustain", "pulse", "wave"],
      },
      peers: [...room.peers.values()].filter((p) => p.id !== clientId).map((p) => ({
        id: p.id, name: p.name, isHost: p.isHost, muted: p.muted, frozen: p.frozen,
      })),
      ts: now,
    }));

    if (buffered > 0) notifyOverflow(socket, peer.overflow);
  };

  socket.onmessage = async (ev) => {
    try {
      const raw = typeof ev.data === "string" ? ev.data : new TextDecoder().decode(ev.data);
      if (raw.length > MAX_BODY_OVERFLOW_BYTES) {
        socket.send(JSON.stringify({ t: "sys", event: "error", error: "too_big" }));
        return;
      }

      let msg: Record<string, unknown>;
      try {
        const parsed = JSON.parse(raw);
        const v = validateEnvelope(parsed);
        if (!v.ok) {
          socket.send(JSON.stringify({ t: "sys", event: "error", error: v.error }));
          return;
        }
        msg = v.data;
      } catch {
        socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_json" }));
        return;
      }

      const peer = room.peers.get(clientId);
      if (!peer) return;
      peer.lastSeen = Date.now();
      peer.recentMsgTimes.push(Date.now());
      updateOverflowState(peer);

      if (msg.sig && room.roomSecret) {
        const { sig, ...rest } = msg;
        const payloadToVerify = JSON.stringify(rest);
        const valid = await verifySignature(room.roomSecret, payloadToVerify, String(sig));
        if (!valid) {
          socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_signature" }));
          return;
        }
      }

      const command = isString(msg.c) ? msg.c as string : "MSG";
      const target  = isString(msg.to) ? msg.to as string : "*";
      const channel = sanitizeChannel(msg.ch);
      const data    = msg.d ?? "";

      if (CHUNK_CMDS.has(command)) {
        const payload = (typeof data === "object" && data !== null)
          ? data as Record<string, unknown>
          : { chunkId: String(data) };
        await handleChunk(roomId, peer, command, payload, socket);
        return;
      }

      if (OVERFLOW_CMDS.has(command)) {
        const payload = (typeof data === "object" && data !== null)
          ? data as Record<string, unknown>
          : {};
        const res = await handleOverflow(roomId, peer, command, payload);
        const cloned = res.clone();
        try {
          const body = await cloned.json();
          socket.send(JSON.stringify({ t: "sys", event: "overflow_ack", cmd: command, result: body }));
        } catch { /* ignore */ }
        return;
      }

      if (command.startsWith("@")) {
        if (GAMEFLOW_CMDS.has(command)) {
          const payload = (typeof data === "object" && data !== null)
            ? data as Record<string, unknown>
            : {};
          const res = await handleGameOverflow(roomId, peer, command, payload);
          const cloned = res.clone();
          try {
            const body = await cloned.json();
            socket.send(JSON.stringify({
              t: "sys", event: "game_overflow_ack", cmd: command, result: body,
            }));
          } catch { /* ignore */ }
          return;
        }

        if (RPC_CMDS.has(command)) {
          const corr = isNumber(msg.corr) ? msg.corr as number : Date.now();
          const res = await handleRpc(roomId, peer, command, target, msg, corr);
          socket.send(JSON.stringify({ t: "sys", event: "rpc_ack", cmd: command, status: res.status }));
          return;
        }

        if (DOOR_CMDS.has(command)) {
          const payload = (typeof data === "object" && data !== null)
            ? data as Record<string, unknown>
            : { sector: String(target).split("-")[0], index: Number(String(target).split("-")[1] ?? 0) };
          const res = await applyDoor(roomId, peer, command, payload);
          socket.send(JSON.stringify({ t: "sys", event: "door_ack", cmd: command, status: res.status }));
          return;
        }

        if (CONTROL_CMDS.has(command)) {
          const res = await applyControl(roomId, clientId, peerName, command, target, String(data));
          socket.send(JSON.stringify({ t: "sys", event: "ctrl_ack", cmd: command, status: res.status }));
          return;
        }

        if (command === "@REGISTER") {
          const capData = data as { name?: string; args?: unknown; desc?: string };
          if (!isString(capData?.name, 256)) {
            socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_cap_name" }));
            return;
          }
          if (!isStringArray(capData.args ?? [])) {
            socket.send(JSON.stringify({ t: "sys", event: "error", error: "bad_cap_args" }));
            return;
          }
          const cap: PeerCapability = {
            name: capData.name as string,
            args: (capData.args ?? []) as string[],
            desc: typeof capData.desc === "string" ? capData.desc.slice(0, 256) : undefined,
            registeredAt: Date.now(),
          };
          registerCapability(room, peer, cap);
          socket.send(JSON.stringify({ t: "sys", event: "cap_registered", cap: cap.name }));
          broadcastToRoom(roomId, {
            t: "sys", event: "cap_updated", peer: clientId, cap: cap.name, ts: Date.now(),
          }, clientId);
          return;
        }

        if (command === "@UNREGISTER") {
          if (isString(data, 256)) {
            peer.capabilities.delete(data as string);
            const set = room.capabilityIndex.get(data as string);
            if (set) { set.delete(clientId); if (set.size === 0) room.capabilityIndex.delete(data as string); }
            socket.send(JSON.stringify({ t: "sys", event: "cap_unregistered", cap: data }));
          }
          return;
        }

        socket.send(JSON.stringify({ t: "sys", event: "error", error: "unknown_command" }));
        return;
      }

      if (peer.muted) {
        socket.send(JSON.stringify({ t: "sys", event: "error", error: "muted" }));
        return;
      }

      const ttl = isNumber(msg.ttl)
        ? Math.max(0, Math.min(msg.ttl as number, MSG_TTL_MS))
        : MSG_TTL_MS;

      const now = Date.now();
      const id = await nextMessageId(roomId);
      const envelope = {
        t: "msg", id, ch: channel,
        f: clientId, n: peerName,
        to: target, c: command, d: data, ts: now,
      };

      const { buffered } = routeThroughOverflow(roomId, peer, envelope);

      if (peer.overflow.active || buffered) {
        await persistOverflow(roomId, clientId, id, envelope);
        peer.overflow.buffered++;
        notifyOverflow(socket, peer.overflow);
        socket.send(JSON.stringify({
          t: "sys", event: "overflow_buffered", id,
          buffered: peer.overflow.buffered, mode: peer.overflow.mode,
        }));
      } else {
        await persist(roomId, id, envelope, ttl);
        if (target === "*") broadcastToRoom(roomId, envelope, clientId);
        else sendToPeer(roomId, target, envelope);
      }

      socket.send(JSON.stringify({
        t: "sys", event: "ack", id, ts: now,
        overflow: peer.overflow.active,
      }));
    } catch (err) {
      console.error("ws_handler_error", err);
      try { socket.send(JSON.stringify({ t: "sys", event: "error", error: "internal" })); }
      catch { /* ignore */ }
    }
  };

  const cleanup = () => {
    const now = Date.now();
    const peer = room.peers.get(clientId);

    if (room.gameOverflow) {
      room.gameOverflow.participants.delete(clientId);
      room.gameOverflow.ready.delete(clientId);
      room.gameOverflow.stats.perPeer.delete(clientId);
      if (
        room.gameOverflow.participants.size === 0 ||
        (room.gameOverflow.active && room.gameOverflow.initiatedBy === clientId)
      ) {
        const s = room.gameOverflow;
        s.active = false;
        broadcastToRoom(roomId, {
          t: "game_overflow",
          cmd: "@GAME_OVERFLOW_ABORTED",
          sessionId: s.id,
          reason: "peer_left",
          ts: Date.now(),
        });
        room.gameOverflow = null;
      }
    }

    unregisterCapabilities(room, clientId);
    room.peers.delete(clientId);
    room.sockets.delete(clientId);

    if (room.host === clientId) {
      const remaining = [...room.peers.values()];
      room.host = remaining.length > 0 ? remaining[0].id : null;
      if (room.host) {
        const newHost = room.peers.get(room.host);
        if (newHost) newHost.isHost = true;
      }
    }

    broadcastToRoom(roomId, {
      t: "presence", event: "leave",
      peer: { id: clientId, name: peer?.name ?? "unknown" },
      host: room.host, ts: now,
    });

    if (room.peers.size === 0 && room.sockets.size === 0) {
      setTimeout(() => {
        const r = rooms.get(roomId);
        if (r && r.peers.size === 0 && r.sockets.size === 0) rooms.delete(roomId);
      }, ROOM_IDLE_MS);
    }
  };

  socket.onclose = cleanup;
  socket.onerror = cleanup;

  return response;
}

async function handlePost(
  req: Request,
  roomId: string,
  clientId: string,
  peerName: string,
): Promise<Response> {
  const room = getRoom(roomId);
  await loadBanList(roomId);

  if (room.banned.has(clientId)) return jsonResponse({ error: "banned" }, 403);

  const lenHeader = req.headers.get("content-length");
  if (lenHeader && parseInt(lenHeader, 10) > MAX_BODY_OVERFLOW_BYTES) {
    return jsonResponse({ error: "payload_too_large" }, 413);
  }

  const body = await req.text();
  if (!body) return jsonResponse({ error: "empty_body" }, 400);
  if (body.length > MAX_BODY_OVERFLOW_BYTES) {
    return jsonResponse({ error: "payload_too_large" }, 413);
  }

  let parsed: Record<string, unknown>;
  try {
    const p = JSON.parse(body);
    parsed = typeof p === "object" && p !== null ? p as Record<string, unknown> : { d: body };
  } catch {
    parsed = { d: body };
  }

  const v = validateEnvelope(parsed);
  if (!v.ok) return jsonResponse({ error: v.error }, 400);
  parsed = v.data;

  const command = isString(parsed.c) ? parsed.c as string : "MSG";
  const target  = isString(parsed.to) ? parsed.to as string : "*";
  const channel = sanitizeChannel(parsed.ch);
  const data    = parsed.d ?? body;

  let peer = room.peers.get(clientId);
  if (!peer) {
    const now = Date.now();
    const isHost = room.host === null;
    if (isHost) room.host = clientId;
    peer = {
      id: clientId, name: peerName,
      joinedAt: now, lastSeen: now,
      isHost, muted: false, frozen: false, banned: false,
      lastKickAt: 0, lastControlAt: 0,
      sessionToken: newSessionToken(),
      capabilities: new Map(), rpcPending: new Map(),
      overflow: newOverflowState(), chunks: new Map(),
      recentMsgTimes: [],
    };
    room.peers.set(clientId, peer);
  }
  peer.lastSeen = Date.now();
  peer.recentMsgTimes.push(Date.now());
  updateOverflowState(peer);

  if (OVERFLOW_CMDS.has(command)) {
    const payload = (typeof data === "object" && data !== null)
      ? data as Record<string, unknown>
      : {};
    return await handleOverflow(roomId, peer, command, payload);
  }

  if (GAMEFLOW_CMDS.has(command)) {
    const payload = (typeof data === "object" && data !== null)
      ? data as Record<string, unknown>
      : {};
    return await handleGameOverflow(roomId, peer, command, payload);
  }

  if (command.startsWith("@")) {
    if (RPC_CMDS.has(command)) {
      const corr = isNumber(parsed.corr) ? parsed.corr as number : Date.now();
      return await handleRpc(roomId, peer, command, target, parsed, corr);
    }
    if (DOOR_CMDS.has(command)) {
      const payload = (typeof data === "object" && data !== null)
        ? data as Record<string, unknown>
        : { sector: "A", index: 0 };
      return await applyDoor(roomId, peer, command, payload);
    }
    if (CONTROL_CMDS.has(command)) {
      return await applyControl(roomId, clientId, peerName, command, target, String(data));
    }
    return jsonResponse({ error: "unknown_command" }, 400);
  }

  if (peer.muted) return jsonResponse({ error: "muted" }, 403);

  const ttl = isNumber(parsed.ttl)
    ? Math.max(0, Math.min(parsed.ttl as number, MSG_TTL_MS))
    : MSG_TTL_MS;

  const now = Date.now();
  const id = await nextMessageId(roomId);
  const envelope = {
    t: "msg", id, ch: channel,
    f: clientId, n: peerName,
    to: target, c: command, d: data, ts: now,
  };

  const { buffered } = routeThroughOverflow(roomId, peer, envelope);

  if (peer.overflow.active || buffered) {
    await persistOverflow(roomId, clientId, id, envelope);
    peer.overflow.buffered++;
    return jsonResponse({
      ok: true, id, ts: now,
      overflow: true,
      buffered: peer.overflow.buffered,
      mode: peer.overflow.mode,
    }, 202, { "Retry-After": "1" });
  }

  await persist(roomId, id, envelope, ttl);
  broadcastToRoom(roomId, envelope, clientId);
  if (target !== "*") sendToPeer(roomId, target, envelope);

  return jsonResponse({ ok: true, id, ts: now, overflow: false });
}

async function handleGet(url: URL, roomId: string, since: number): Promise<Response> {
  const channel = sanitizeChannel(url.searchParams.get("ch"));

  const messages: unknown[] = [];
  let lastId = since;

  const iter = kv.list(
    { prefix: ["msg", roomId] },
    { start: ["msg", roomId, since + 1], limit: MAX_MESSAGES_POLL },
  );

  for await (const entry of iter) {
    const v = entry.value as { id?: number; ch?: string };
    if (typeof v?.id === "number" && v.id > lastId) lastId = v.id;
    if (channel !== "default" && v?.ch !== channel) continue;
    messages.push(entry.value);
  }

  const room = rooms.get(roomId);
  const peers = room ? [...room.peers.values()].map((p) => ({
    id: p.id, name: p.name, isHost: p.isHost,
    muted: p.muted, frozen: p.frozen, lastSeen: p.lastSeen,
    caps: [...p.capabilities.keys()],
    overflow: p.overflow.active ? p.overflow.mode : "normal",
  })) : [];
  const controls = room
    ? room.controlLog.filter((c) => c.ts > Date.now() - 30000)
    : [];

  return jsonResponse({
    messages,
    lastId,
    count: messages.length,
    channel,
    peers,
    host: room?.host ?? null,
    openControl: room?.openControl ?? true,
    controls,
    gameOverflowActive: room?.gameOverflow?.active ?? false,
    ts: Date.now(),
  });
}

async function handleDelete(roomId: string): Promise<Response> {
  const keys: Deno.KvKey[] = [];
  for await (const e of kv.list({ prefix: ["msg", roomId] }, { limit: 5000 })) keys.push(e.key);
  for await (const e of kv.list({ prefix: ["door", roomId] }, { limit: 5000 })) keys.push(e.key);
  for await (const e of kv.list({ prefix: ["overflow", roomId] }, { limit: 10000 })) keys.push(e.key);
  await Promise.all(keys.map((k) => kv.delete(k)));
  await kv.delete(["counter", roomId]);
  rooms.delete(roomId);
  rpcRegistry.delete(roomId);
  return jsonResponse({ ok: true, cleared: keys.length });
}

async function handleDoors(roomId: string, url: URL): Promise<Response> {
  const room = getRoom(roomId);
  const sectorFilter = url.searchParams.get("sector");
  const sinceParam = url.searchParams.get("since");
  const since = sinceParam ? parseInt(sinceParam, 10) : 0;

  const out: DoorState[] = [];
  for (const d of room.doorCache.values()) {
    if (sectorFilter && d.sector !== sectorFilter.toUpperCase()) continue;
    if (since && d.updatedAt < since) continue;
    out.push(d);
  }

  out.sort((a, b) => {
    if (a.sector !== b.sector) return a.sector.localeCompare(b.sector);
    return a.index - b.index;
  });

  const expectedTotal = room.floorType === "archives"
    ? ARCHIVES_TOTAL_DOORS
    : room.floorType === "rooms"
    ? ROOMS_TOTAL_DOORS
    : null;

  return jsonResponse({
    ok: true,
    room: roomId,
    floorType: room.floorType,
    doors: out,
    count: out.length,
    maxDoorSeen: room.maxDoorSeen,
    targetDoors: expectedTotal,
    progress: expectedTotal ? Math.min(1, room.maxDoorSeen / expectedTotal) : null,
    archivesStructure: {
      totalDoors: ARCHIVES_TOTAL_DOORS,
      sectors: ARCHIVES_SECTORS,
      doorsPerSector: ARCHIVES_DOORS_PER_SEC,
    },
    ts: Date.now(),
  });
}

async function handleDoorSync(
  req: Request,
  roomId: string,
  clientId: string,
): Promise<Response> {
  const room = getRoom(roomId);
  const body = await req.text();
  if (body.length > MAX_BODY_BYTES) return jsonResponse({ error: "payload_too_large" }, 413);

  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(body);
  } catch {
    return jsonResponse({ error: "bad_json" }, 400);
  }

  const peer = room.peers.get(clientId);
  if (!peer) return jsonResponse({ error: "not_in_room" }, 403);

  const floorType = typeof parsed.floorType === "string" ? parsed.floorType : "unknown";
  if (["hotel", "archives", "rooms", "mines"].includes(floorType)) {
    room.floorType = floorType as Room["floorType"];
  }

  const result = await applyDoor(roomId, peer, "@DOOR_SYNC", parsed);
  return result;
}

async function handleOverflowEndpoint(
  roomId: string,
  clientId: string,
  url: URL,
): Promise<Response> {
  const room = getRoom(roomId);
  const peer = room.peers.get(clientId);
  if (!peer) return jsonResponse({ error: "not_in_room" }, 403);

  const action = url.searchParams.get("action") ?? "status";

  if (action === "status") {
    return await handleOverflow(roomId, peer, "@OVERFLOW_STATUS", {});
  }
  if (action === "drain") {
    const limit = parseInt(url.searchParams.get("limit") ?? String(OVERFLOW_DRAIN_BATCH), 10);
    return await handleOverflow(roomId, peer, "@OVERFLOW_DRAIN", { limit });
  }
  if (action === "clear") {
    return await handleOverflow(roomId, peer, "@OVERFLOW_CLEAR", {});
  }
  if (action === "stats") {
    return await handleOverflow(roomId, peer, "@OVERFLOW_STATS", {});
  }
  if (action === "resume") {
    const since = parseInt(url.searchParams.get("since") ?? "0", 10);
    return await handleOverflow(roomId, peer, "@OVERFLOW_RESUME", { since });
  }

  return jsonResponse({ error: "unknown_action" }, 400);
}

async function handleGameOverflowEndpoint(
  roomId: string,
  clientId: string,
  url: URL,
): Promise<Response> {
  const room = getRoom(roomId);
  const peer = room.peers.get(clientId);
  if (!peer) return jsonResponse({ error: "not_in_room" }, 403);

  const action = url.searchParams.get("action") ?? "status";

  if (action === "status") {
    return await handleGameOverflow(roomId, peer, "@GAME_OVERFLOW_STATUS", {});
  }
  if (action === "begin") {
    return await handleGameOverflow(roomId, peer, "@GAME_OVERFLOW_BEGIN", {
      mode: url.searchParams.get("mode") ?? "burst",
      target: url.searchParams.get("target") ?? "chat",
      intensity: parseInt(url.searchParams.get("intensity") ?? "5", 10),
      durationMs: parseInt(url.searchParams.get("duration") ?? "5000", 10),
    });
  }
  if (action === "ready") {
    return await handleGameOverflow(roomId, peer, "@GAME_OVERFLOW_READY", {});
  }
  if (action === "stop") {
    return await handleGameOverflow(roomId, peer, "@GAME_OVERFLOW_STOP", {
      reason: url.searchParams.get("reason") ?? "manual",
    });
  }

  return jsonResponse({ error: "unknown_action" }, 400);
}

async function handlePeers(roomId: string): Promise<Response> {
  const room = rooms.get(roomId);
  const peers = room ? [...room.peers.values()] : [];
  return jsonResponse({
    ok: true,
    peers: peers.map((p) => ({
      id: p.id, name: p.name, isHost: p.isHost,
      muted: p.muted, frozen: p.frozen, lastSeen: p.lastSeen,
      joinedAt: p.joinedAt, caps: [...p.capabilities.keys()],
      overflow: {
        active: p.overflow.active,
        mode: p.overflow.mode,
        buffered: p.overflow.buffered,
        rate: computeCurrentRate(p),
      },
    })),
    count: peers.length,
    host: room?.host ?? null,
    banned: room ? [...room.banned] : [],
    floorType: room?.floorType ?? "unknown",
    maxDoorSeen: room?.maxDoorSeen ?? 0,
  });
}

async function handleState(roomId: string): Promise<Response> {
  const room = getRoom(roomId);
  return jsonResponse({
    ok: true, id: roomId, host: room.host,
    openControl: room.openControl,
    hasRoomSecret: room.roomSecret !== null,
    floorType: room.floorType,
    maxDoorSeen: room.maxDoorSeen,
    doorsInCache: room.doorCache.size,
    archivesStructure: {
      totalDoors: ARCHIVES_TOTAL_DOORS,
      sectors: ARCHIVES_SECTORS,
      doorsPerSector: ARCHIVES_DOORS_PER_SEC,
    },
    overflow: {
      peersActive: [...room.peers.values()].filter((p) => p.overflow.active).length,
      recentLog: room.overflowLog.slice(-20),
      config: {
        trigger: RATE_OVERFLOW_TRIGGER,
        exit: RATE_OVERFLOW_EXIT,
        bufferMax: OVERFLOW_BUFFER_MAX,
        chunkSize: CHUNK_SIZE,
      },
    },
    gameOverflow: room.gameOverflow
      ? {
          id: room.gameOverflow.id,
          mode: room.gameOverflow.mode,
          target: room.gameOverflow.target,
          active: room.gameOverflow.active,
          intensity: room.gameOverflow.intensity,
          participants: [...room.gameOverflow.participants],
          ready: [...room.gameOverflow.ready],
          totalTicks: room.gameOverflow.stats.totalTicks,
        }
      : null,
    peers: [...room.peers.values()].map((p) => ({
      id: p.id, name: p.name, isHost: p.isHost,
      muted: p.muted, frozen: p.frozen, lastSeen: p.lastSeen,
      caps: [...p.capabilities.keys()],
      rpcPending: p.rpcPending.size,
      overflow: p.overflow.active ? p.overflow.mode : "normal",
      rate: computeCurrentRate(p),
    })),
    banned: [...room.banned],
    bannedNames: [...room.bannedNames],
    controls: room.controlLog.slice(-50),
    capabilityCount: room.capabilityIndex.size,
    createdAt: room.createdAt,
    ts: Date.now(),
  });
}

async function handleMode(url: URL, roomId: string): Promise<Response> {
  const room = getRoom(roomId);
  const open = url.searchParams.get("open");
  if (open === "true")  room.openControl = true;
  if (open === "false") room.openControl = false;
  return jsonResponse({ ok: true, openControl: room.openControl });
}

async function handleBans(roomId: string): Promise<Response> {
  const room = getRoom(roomId);
  await loadBanList(roomId);
  return jsonResponse({
    ok: true,
    banned: [...room.banned],
    bannedNames: [...room.bannedNames],
  });
}

async function handleCaps(roomId: string): Promise<Response> {
  const room = rooms.get(roomId);
  if (!room) return jsonResponse({ ok: true, capabilities: [] });

  const caps = [...room.capabilityIndex.entries()].map(([name, holders]) => {
    const sample = room.peers.get([...holders][0]);
    const meta = sample?.capabilities.get(name);
    return { name, holders: [...holders], args: meta?.args ?? [], desc: meta?.desc };
  });

  return jsonResponse({ ok: true, capabilities: caps, count: caps.length });
}

function cleanupRpcRegistry(): void {
  const now = Date.now();
  for (const [roomId, map] of rpcRegistry) {
    for (const [corr, info] of map) {
      if (now - info.ts > RPC_TIMEOUT_MS) map.delete(corr);
    }
    if (map.size === 0) rpcRegistry.delete(roomId);
  }
}

function cleanupAllChunks(): void {
  for (const room of rooms.values()) {
    for (const peer of room.peers.values()) {
      cleanupChunks(peer);
    }
  }
}

setInterval(cleanupRpcRegistry, 10_000);
setInterval(cleanupAllChunks, 15_000);

Deno.serve(async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  const pathname = url.pathname;

  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });

  if (pathname === "/" || pathname === "/health") {
    return jsonResponse({
      ok: true, service: "mini-server", version: "7.0.0",
      rooms: rooms.size, ws: true, control: true, rpc: true,
      hmac: true, caps: true, doors: true, overflow: true, chunks: true,
      gameOverflow: true,
      archives: {
        totalDoors: ARCHIVES_TOTAL_DOORS,
        sectors: ARCHIVES_SECTORS,
        doorsPerSector: ARCHIVES_DOORS_PER_SEC,
      },
      roomsClassic: ROOMS_TOTAL_DOORS,
      overflowConfig: {
        trigger: RATE_OVERFLOW_TRIGGER,
        exit: RATE_OVERFLOW_EXIT,
        bufferMax: OVERFLOW_BUFFER_MAX,
        chunkSize: CHUNK_SIZE,
        maxChunks: MAX_CHUNKS,
      },
      gameOverflowConfig: {
        maxDurationMs: GAMEFLOW_MAX_DURATION_MS,
        maxIntensity: GAMEFLOW_MAX_INTENSITY,
        syncLeadMs: GAMEFLOW_SYNC_LEAD_MS,
        readyTimeoutMs: GAMEFLOW_READY_TIMEOUT,
        modes: ["burst", "sustain", "pulse", "wave"],
      },
      ts: Date.now(),
    });
  }

  if (pathname === "/stats") {
    const snapshot: Array<{
      id: string; peers: number; sockets: number; host: string | null;
      caps: number; doors: number; floorType: string; overflowing: number;
      gameOverflowActive: boolean;
    }> = [];
    for (const [id, r] of rooms) {
      snapshot.push({
        id, peers: r.peers.size, sockets: r.sockets.size,
        host: r.host, caps: r.capabilityIndex.size,
        doors: r.doorCache.size, floorType: r.floorType,
        overflowing: [...r.peers.values()].filter((p) => p.overflow.active).length,
        gameOverflowActive: r.gameOverflow?.active ?? false,
      });
    }
    return jsonResponse({
      ok: true, rooms: snapshot, rateBuckets: rateBuckets.size,
      rpcRooms: rpcRegistry.size, ts: Date.now(),
    });
  }

  const segments = pathname.slice(1).split("/");
  const roomId = sanitizeRoom(segments[0] ?? "");
  if (!roomId) return jsonResponse({ error: "invalid_room" }, 400);

  const clientId = sanitizeClient(
    req.headers.get("X-Client-Id") ??
    req.headers.get("X-Peer-Id") ??
    `anon_${Math.random().toString(36).slice(2, 10)}`,
  ) ?? "anon";

  const peerName = sanitizeName(
    url.searchParams.get("name") ??
    req.headers.get("X-Client-Name") ??
    clientId,
  );

  const rl = checkRateLimit(clientId);
  if (!rl.ok) {
    return jsonResponse(
      { error: "rate_limited", count: rl.count, max: RATE_MAX_REQUESTS },
      429,
      { "Retry-After": "1" },
    );
  }

  const isWebSocket = req.headers.get("upgrade")?.toLowerCase() === "websocket";
  if (isWebSocket) {
    try { return await handleWebSocket(req, roomId, clientId, peerName); }
    catch { return jsonResponse({ error: "ws_upgrade_failed" }, 500); }
  }

  const sub = segments[1] ?? "";

  try {
    if (sub === "peers")        return await handlePeers(roomId);
    if (sub === "state")        return await handleState(roomId);
    if (sub === "bans")         return await handleBans(roomId);
    if (sub === "caps")         return await handleCaps(roomId);
    if (sub === "mode")         return await handleMode(url, roomId);
    if (sub === "doors")        return await handleDoors(roomId, url);
    if (sub === "sync")         return await handleDoorSync(req, roomId, clientId);
    if (sub === "overflow")     return await handleOverflowEndpoint(roomId, clientId, url);
    if (sub === "gameoverflow") return await handleGameOverflowEndpoint(roomId, clientId, url);

    switch (req.method) {
      case "POST": return await handlePost(req, roomId, clientId, peerName);
      case "GET": {
        const since = Math.max(0, parseInt(url.searchParams.get("since") ?? "0", 10) || 0);
        return await handleGet(url, roomId, since);
      }
      case "DELETE": return await handleDelete(roomId);
      case "HEAD":   return new Response(null, { status: 200, headers: CORS });
    }
  } catch (err) {
    console.error("relay_error", err);
    return jsonResponse({ error: "internal_error" }, 500);
  }

  return jsonResponse(
    { error: "method_not_allowed" },
    405,
    { Allow: "GET, POST, DELETE, OPTIONS, HEAD" },
  );
});

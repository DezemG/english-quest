/* =====================================================================
   English Quest — Multiplayer Server
   Node.js + ws (WebSocket). Serves the game client and relays player
   state so everyone in the same map sees each other in real time.
   ---------------------------------------------------------------------
   Run locally:   npm install   then   npm start
   Open:          http://localhost:3000
   Deploy:        see DEPLOY-GUIDE.html (Render / Railway / Glitch)
   ===================================================================== */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;

/* ---------- static file serving (the game client) ---------- */
const CLIENT = path.join(__dirname, "public", "index.html");
const server = http.createServer((req, res) => {
  // health check for hosting platforms
  if (req.url === "/healthz") { res.writeHead(200); res.end("ok"); return; }
  // everything else serves the single-page client
  fs.readFile(CLIENT, (err, data) => {
    if (err) { res.writeHead(500); res.end("client not found"); return; }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(data);
  });
});

/* ---------- multiplayer state ---------- */
const wss = new WebSocketServer({ server });
const players = new Map(); // id -> {id,name,map,x,y,dir,frame,lvl,look}
let nextId = 1;

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function peersFor(id) {
  const me = players.get(id);
  if (!me) return [];
  const out = [];
  for (const [pid, p] of players) {
    if (pid === id || p.map !== me.map) continue;
    out.push({ id: p.id, name: p.name, x: p.x, y: p.y, dir: p.dir, frame: p.frame, lvl: p.lvl, look: p.look });
  }
  return out;
}
// tell everyone in `mapName` (optionally excluding `exceptId`) to refresh their peer list
function broadcastMap(mapName, exceptId) {
  for (const [pid, p] of players) {
    if (p.map !== mapName || pid === exceptId) continue;
    send(p.ws, { t: "peers", peers: peersForRaw(pid) });
  }
}
function peersForRaw(id) { return peersFor(id); }

wss.on("connection", (ws) => {
  const id = "p" + (nextId++);
  ws._pid = id;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.t === "join") {
      players.set(id, {
        id, ws,
        name: (msg.name || "ผู้เล่น").slice(0, 16),
        map: msg.map || "farm",
        x: msg.x || 320, y: msg.y || 256,
        dir: msg.dir || "down", frame: 0,
        lvl: msg.lvl || 1,
        look: msg.look || {}
      });
      // confirm to the newcomer with their id + current peers
      send(ws, { t: "welcome", id, peers: peersFor(id) });
      // notify others in the same map
      broadcastMap(players.get(id).map, id);
      return;
    }

    const p = players.get(id);
    if (!p) return;

    if (msg.t === "move") {
      const prevMap = p.map;
      if (typeof msg.x === "number") p.x = msg.x;
      if (typeof msg.y === "number") p.y = msg.y;
      if (msg.dir) p.dir = msg.dir;
      if (typeof msg.frame === "number") p.frame = msg.frame;
      if (typeof msg.lvl === "number") p.lvl = msg.lvl;
      if (msg.map && msg.map !== prevMap) {
        p.map = msg.map;
        // both the old and new room need to refresh
        broadcastMap(prevMap, id);
        broadcastMap(p.map, id);
        send(ws, { t: "peers", peers: peersFor(id) });
      } else {
        // normal movement: push fresh peer lists to others in the room
        broadcastMap(p.map, id);
      }
      return;
    }

    if (msg.t === "look") { // appearance changed mid-game
      p.look = msg.look || p.look;
      if (msg.name) p.name = msg.name.slice(0, 16);
      broadcastMap(p.map, id);
      return;
    }

    if (msg.t === "chat") {
      const text = ("" + (msg.text || "")).slice(0, 80);
      const me = players.get(id);
      for (const [, q] of players) {
        if (q.map === me.map) send(q.ws, { t: "chat", from: me.name, id, text });
      }
      return;
    }
  });

  const cleanup = () => {
    const p = players.get(id);
    const mapName = p ? p.map : null;
    players.delete(id);
    if (mapName) broadcastMap(mapName);
  };
  ws.on("close", cleanup);
  ws.on("error", cleanup);
});

/* ---------- heartbeat: drop dead connections ---------- */
setInterval(() => {
  for (const [id, p] of players) {
    if (p.ws.readyState !== p.ws.OPEN) {
      players.delete(id);
      broadcastMap(p.map);
    }
  }
}, 15000);

server.listen(PORT, () => {
  console.log(`English Quest multiplayer server running on port ${PORT}`);
});

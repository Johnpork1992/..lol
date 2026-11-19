/**
 * Hogwarts Wizard Duel - Multiplayer Server
 * Simple WebSocket relay + room/team management
 * Run: node server.js
 * Default port 3847
 */
const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3847;
const TICK = 50; // ms, ~20 Hz state broadcast

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Hogwarts Wizard Duel Multiplayer Server\nConnect via WebSocket.');
});

const wss = new WebSocketServer({ server });

/** @type {Map<string, Room>} */
const rooms = new Map();

function uid() {
  return crypto.randomBytes(4).toString('hex');
}

class Player {
  constructor(ws, id) {
    this.ws = ws;
    this.id = id;
    this.name = 'Wizard';
    this.team = 0;
    this.ready = false;
    this.color = '#c9a13a';
    this.x = 0;
    this.y = 0;
    this.vx = 0;
    this.vy = 0;
    this.ang = 0;
    this.hp = 100;
    this.mhp = 100;
    this.mp = 100;
    this.sh = 0;
    this.shT = 0;
    this.st = {};
    this.inv = 0;
    this.hs = 0;
    this.bo = 0;
    this.rf = 0;
    this.disarmed = 0;
    this.sel = 0;
    this.alive = true;
    this.lastInput = 0;
  }

  snapshot() {
    return {
      id: this.id,
      name: this.name,
      team: this.team,
      ready: this.ready,
      color: this.color,
      x: this.x,
      y: this.y,
      vx: this.vx,
      vy: this.vy,
      ang: this.ang,
      hp: this.hp,
      mhp: this.mhp,
      mp: this.mp,
      sh: this.sh,
      shT: this.shT,
      st: this.st,
      inv: this.inv,
      hs: this.hs,
      bo: this.bo,
      rf: this.rf,
      disarmed: this.disarmed,
      sel: this.sel,
      alive: this.alive,
    };
  }
}

class Room {
  constructor(code) {
    this.code = code;
    this.players = new Map();
    this.hostId = null;
    this.numTeams = 2;
    this.mode = 'teams'; // 'teams' | 'ffa'
    this.state = 'lobby'; // lobby | playing | ended
    this.tickTimer = null;
    this.spawned = false;
  }

  add(player) {
    this.players.set(player.id, player);
    if (!this.hostId) this.hostId = player.id;
    this.broadcast({ t: 'players', list: this.playerList() }, null);
  }

  remove(id) {
    this.players.delete(id);
    if (this.hostId === id) {
      this.hostId = this.players.keys().next().value || null;
    }
    if (this.players.size === 0) {
      this.stopTick();
      rooms.delete(this.code);
      return;
    }
    this.broadcast({ t: 'players', list: this.playerList() }, null);
    this.broadcast({ t: 'left', id }, null);
  }

  playerList() {
    return [...this.players.values()].map((p) => ({
      id: p.id,
      name: p.name,
      team: p.team,
      ready: p.ready,
      color: p.color,
      isHost: p.id === this.hostId,
    }));
  }

  broadcast(msg, exceptId) {
    const data = JSON.stringify(msg);
    for (const p of this.players.values()) {
      if (p.id !== exceptId && p.ws.readyState === 1) {
        p.ws.send(data);
      }
    }
  }

  send(toId, msg) {
    const p = this.players.get(toId);
    if (p && p.ws.readyState === 1) p.ws.send(JSON.stringify(msg));
  }

  startTick() {
    if (this.tickTimer) return;
    this.tickTimer = setInterval(() => this.tick(), TICK);
  }

  stopTick() {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
  }

  tick() {
    if (this.state !== 'playing') return;
    const states = [...this.players.values()].map((p) => p.snapshot());
    this.broadcast({ t: 'state', players: states, ts: Date.now() }, null);
  }

  startGame() {
    if (this.state !== 'lobby') return;
    this.state = 'playing';
    this.spawned = true;
    // Assign spawn offsets by team
    const teamSpawns = {};
    let teamIdx = 0;
    for (const p of this.players.values()) {
      if (!teamSpawns[p.team]) {
        teamSpawns[p.team] = teamIdx++;
      }
    }
    // Basic spawn positions around center courtyard-ish
    const base = [
      [50 * 64, 70 * 64],
      [45 * 64, 65 * 64],
      [55 * 64, 65 * 64],
      [40 * 64, 72 * 64],
      [60 * 64, 72 * 64],
      [48 * 64, 75 * 64],
      [52 * 64, 75 * 64],
      [42 * 64, 68 * 64],
    ];
    let i = 0;
    for (const p of this.players.values()) {
      const sp = base[i % base.length];
      p.x = sp[0] + (Math.random() - 0.5) * 80;
      p.y = sp[1] + (Math.random() - 0.5) * 80;
      p.hp = p.mhp = 100;
      p.mp = 100;
      p.sh = 0;
      p.st = {};
      p.alive = true;
      p.disarmed = 0;
      i++;
    }
    this.broadcast({
      t: 'start',
      numTeams: this.numTeams,
      mode: this.mode,
      players: [...this.players.values()].map((p) => p.snapshot()),
    }, null);
    this.startTick();
  }
}

function getOrCreateRoom(code) {
  if (!code) code = uid().slice(0, 5).toUpperCase();
  if (!rooms.has(code)) rooms.set(code, new Room(code));
  return rooms.get(code);
}

wss.on('connection', (ws) => {
  const id = uid();
  let player = new Player(ws, id);
  let room = null;

  ws.send(JSON.stringify({ t: 'welcome', id, server: 'HWD-MP/1' }));

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    const type = msg.t;

    if (type === 'join') {
      if (room) return;
      const code = (msg.code || '').toUpperCase().trim() || null;
      room = getOrCreateRoom(code);
      player.name = (msg.name || 'Wizard').slice(0, 16);
      player.color = msg.color || '#c9a13a';
      player.team = Math.max(0, Math.min(7, msg.team | 0));
      room.add(player);
      ws.send(JSON.stringify({
        t: 'joined',
        id,
        room: room.code,
        host: room.hostId === id,
        numTeams: room.numTeams,
        mode: room.mode,
        players: room.playerList(),
        state: room.state,
      }));
      return;
    }

    if (!room) return;

    if (type === 'setName') {
      player.name = (msg.name || player.name).slice(0, 16);
      room.broadcast({ t: 'players', list: room.playerList() }, null);
    } else if (type === 'setTeam') {
      player.team = Math.max(0, Math.min(7, msg.team | 0));
      player.ready = false;
      room.broadcast({ t: 'players', list: room.playerList() }, null);
    } else if (type === 'setNumTeams' && room.hostId === id) {
      room.numTeams = Math.max(1, Math.min(4, msg.n | 0));
      room.mode = room.numTeams <= 1 ? 'ffa' : 'teams';
      // Rebalance teams if needed
      room.broadcast({ t: 'config', numTeams: room.numTeams, mode: room.mode }, null);
      room.broadcast({ t: 'players', list: room.playerList() }, null);
    } else if (type === 'ready') {
      player.ready = !!msg.ready;
      room.broadcast({ t: 'players', list: room.playerList() }, null);
    } else if (type === 'start' && room.hostId === id && room.state === 'lobby') {
      const allReady = [...room.players.values()].every((p) => p.ready || p.id === id);
      if (room.players.size >= 1) {
        room.startGame();
      }
    } else if (type === 'input' && room.state === 'playing') {
      // Authoritative position from client (trust for now; can add validation later)
      if (typeof msg.x === 'number') player.x = msg.x;
      if (typeof msg.y === 'number') player.y = msg.y;
      if (typeof msg.vx === 'number') player.vx = msg.vx;
      if (typeof msg.vy === 'number') player.vy = msg.vy;
      if (typeof msg.ang === 'number') player.ang = msg.ang;
      if (typeof msg.hp === 'number') player.hp = msg.hp;
      if (typeof msg.mp === 'number') player.mp = msg.mp;
      if (typeof msg.sh === 'number') player.sh = msg.sh;
      if (msg.st) player.st = msg.st;
      if (typeof msg.inv === 'number') player.inv = msg.inv;
      if (typeof msg.hs === 'number') player.hs = msg.hs;
      if (typeof msg.bo === 'number') player.bo = msg.bo;
      if (typeof msg.rf === 'number') player.rf = msg.rf;
      if (typeof msg.disarmed === 'number') player.disarmed = msg.disarmed;
      if (typeof msg.sel === 'number') player.sel = msg.sel;
      if (typeof msg.alive === 'boolean') player.alive = msg.alive;
      player.lastInput = Date.now();
    } else if (type === 'cast' && room.state === 'playing') {
      // Relay cast event so everyone sees the exact spell + aim
      room.broadcast({
        t: 'cast',
        from: id,
        spell: msg.spell, // index or name
        tx: msg.tx,
        ty: msg.ty,
        name: msg.name,
        color: msg.color,
        k: msg.k,
        words: msg.words,
        ts: Date.now(),
      }, null); // include self so local can optionally confirm
    } else if (type === 'effect' && room.state === 'playing') {
      // Status / damage application broadcast (from the one who applied it)
      room.broadcast({
        t: 'effect',
        from: id,
        to: msg.to,
        kind: msg.kind, // 'hit' | 'status' | 'heal' etc
        dmg: msg.dmg,
        fx: msg.fx,
        spell: msg.spell,
        ts: Date.now(),
      }, id);
    } else if (type === 'chat') {
      room.broadcast({
        t: 'chat',
        from: id,
        name: player.name,
        text: String(msg.text || '').slice(0, 120),
      }, null);
    } else if (type === 'rematch' && room.hostId === id) {
      room.state = 'lobby';
      room.stopTick();
      for (const p of room.players.values()) {
        p.ready = false;
        p.alive = true;
        p.hp = p.mhp;
        p.mp = 100;
        p.st = {};
      }
      room.broadcast({ t: 'lobby', players: room.playerList() }, null);
    }
  });

  ws.on('close', () => {
    if (room) room.remove(id);
  });
});

server.listen(PORT, () => {
  console.log(`Hogwarts Wizard Duel MP server listening on ws://0.0.0.0:${PORT}`);
});

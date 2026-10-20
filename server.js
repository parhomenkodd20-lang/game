'use strict';
// =====================================================================
// Игровой сервер Nature Walk (без внешних библиотек — только Node.js 18+).
//
// Что делает:
//  • принимает игроков по WebSocket (обычный порт 443/80, поэтому не мешают NAT и блокировки WebRTC);
//  • несколько миров (комнат) в одном процессе — игрок выбирает мир в меню;
//  • быстро рассылает позиции игроков сам (только то, что нужно: близкие — часто, далёкие — редко);
//  • остальные события (стройка, удары, сумки) пересылает «хозяину мира» — как и раньше;
//  • хранит сохранение мира (постройки, срубленные деревья…) и отдаёт его новому хозяину.
// =====================================================================
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = parseInt(process.env.PORT, 10) || 8080;
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS, 10) || 100;           // игроков в одном мире
const WORLD_NAMES = (process.env.WORLDS || 'Мир 1,Мир 2,Мир 3').split(',').map(s => s.trim()).filter(Boolean);
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const UPSTASH_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, '');
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || '';

const TICK_MS = 50;              // как часто рассылаем позиции (20 раз в секунду)
const MAX_FRAME = 4 * 1024 * 1024; // самое большое сообщение (сохранение мира от хозяина)
const MAX_PLAYER_MSG = 32 * 1024;  // обычный игрок не может слать больше
const MAX_SAVE_BYTES = 3 * 1024 * 1024;
const SAVE_DELAY_MS = 5000;
const HELD_ITEMS = ['stone', 'pickaxe', 'axe', 'mallet', 'torch', 'chertyozh', 'spear'];

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ---------------------------------------------------------------------
// Хранилище сохранений: файл (если диск не стирается) и/или Upstash Redis (бесплатно, переживает перезапуски Render)
// ---------------------------------------------------------------------
const store = {
    async load(id) {
        if (UPSTASH_URL && UPSTASH_TOKEN) {
            try {
                const r = await fetch(UPSTASH_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN }, body: JSON.stringify(['GET', 'nw:' + id]) });
                const j = await r.json();
                if (j && typeof j.result === 'string') return JSON.parse(j.result);
            } catch (e) { console.error('load upstash', id, e.message); }
        }
        try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, id + '.json'), 'utf8')); } catch (_) { return null; }
    },
    async save(id, data) {
        const text = JSON.stringify(data);
        try {
            fs.mkdirSync(DATA_DIR, { recursive: true });
            const f = path.join(DATA_DIR, id + '.json');
            fs.writeFileSync(f + '.tmp', text); fs.renameSync(f + '.tmp', f);
        } catch (e) { console.error('save file', id, e.message); }
        if (UPSTASH_URL && UPSTASH_TOKEN) {
            try {
                const r = await fetch(UPSTASH_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN }, body: JSON.stringify(['SET', 'nw:' + id, text]) });
                if (!r.ok) console.error('save upstash', id, r.status);
            } catch (e) { console.error('save upstash', id, e.message); }
        }
    }
};

// ---------------------------------------------------------------------
// Миры
// ---------------------------------------------------------------------
const worlds = new Map(); // id -> { id, name, host, players: Map(id -> Conn), saved, saveTimer, dirty }
WORLD_NAMES.forEach((name, i) => worlds.set('w' + (i + 1), { id: 'w' + (i + 1), name, host: null, players: new Map(), saved: null, saveTimer: null, dirty: false }));

function schedulePersist(w) {
    w.dirty = true;
    if (w.saveTimer) return;
    w.saveTimer = setTimeout(() => { w.saveTimer = null; persistNow(w); }, SAVE_DELAY_MS);
}
async function persistNow(w) {
    if (!w.dirty || !w.saved) return;
    w.dirty = false;
    await store.save(w.id, w.saved);
}

// ---------------------------------------------------------------------
// WebSocket (RFC 6455), написан вручную, чтобы не зависеть от пакетов
// ---------------------------------------------------------------------
function encodeFrame(opcode, payload) {
    const len = payload.length;
    let head;
    if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
    else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
    head[0] = 0x80 | opcode;
    return Buffer.concat([head, payload]);
}
const textFrame = (str) => encodeFrame(0x1, Buffer.from(str, 'utf8'));

let nextConnNo = 1;
class Conn {
    constructor(socket) {
        this.no = nextConnNo++;
        this.socket = socket; this.buf = Buffer.alloc(0); this.frag = null; this.fragSize = 0;
        this.alive = true; this.lastPong = Date.now();
        this.world = null; this.id = null; this.isHost = false; this.attached = false;
        this.state = null; this.ver = 0; this.nick = ''; this.lastFwd = 0; this.fwdPending = false;
        this.sentVer = new Map(); this.sentNick = new Map(); // что уже отправлено этому игроку
        this.tokens = 300; this.tokensAt = Date.now();
        socket.on('data', (c) => this.onData(c));
        socket.on('close', () => this.onClose());
        socket.on('error', () => this.kill());
        socket.setNoDelay(true);
    }
    sendFrame(frame) {
        if (!this.alive) return;
        if (this.socket.writableLength > 8 * 1024 * 1024) { this.kill(); return; } // клиент не успевает читать
        try { this.socket.write(frame); } catch (_) { this.kill(); }
    }
    send(obj) { this.sendFrame(textFrame(JSON.stringify(obj))); }
    kill() { if (!this.alive) return; try { this.socket.destroy(); } catch (_) {} this.onClose(); }
    onData(chunk) {
        this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
        for (;;) {
            const b = this.buf;
            if (b.length < 2) return;
            const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
            let len = b[1] & 0x7f, off = 2;
            if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
            else if (len === 127) { if (b.length < 10) return; const big = b.readBigUInt64BE(2); if (big > BigInt(MAX_FRAME)) return this.kill(); len = Number(big); off = 10; }
            if (len > MAX_FRAME || !masked) return this.kill(); // клиент обязан маскировать данные
            if (b.length < off + 4 + len) return;
            const mask = b.subarray(off, off + 4);
            const payload = Buffer.from(b.subarray(off + 4, off + 4 + len));
            for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
            this.buf = b.subarray(off + 4 + len);
            if (op === 0x8) { try { this.socket.end(encodeFrame(0x8, Buffer.alloc(0))); } catch (_) {} this.kill(); return; }
            if (op === 0x9) { this.sendFrame(encodeFrame(0xA, payload)); continue; }
            if (op === 0xA) { this.lastPong = Date.now(); continue; }
            if (op === 0x1 || op === 0x0) {
                if (op === 0x1) { this.frag = [payload]; this.fragSize = payload.length; }
                else if (this.frag) { this.frag.push(payload); this.fragSize += payload.length; }
                else continue;
                if (this.fragSize > MAX_FRAME) return this.kill();
                if (fin) { const text = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.onText(text); }
            }
        }
    }
    onText(text) {
        const now = Date.now();
        this.tokens = Math.min(this.isHost ? 6000 : 300, this.tokens + (now - this.tokensAt) * (this.isHost ? 5 : 0.15)); this.tokensAt = now;
        if (--this.tokens < 0) { this.tokens = 0; return; }              // слишком часто шлёт — игнорируем лишнее
        if (!this.isHost && text.length > MAX_PLAYER_MSG) return;
        let m; try { m = JSON.parse(text); } catch (_) { return; }
        if (!m || typeof m !== 'object') return;
        try { handleMessage(this, m); } catch (e) { console.error('handleMessage', e); }
    }
    onClose() {
        if (!this.alive) return;
        this.alive = false;
        leaveWorld(this);
    }
}

// ---------------------------------------------------------------------
// Логика
// ---------------------------------------------------------------------
function cleanNick(s) {
    return Array.from(String(s || '').replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim()).slice(0, 15).join('').trim();
}
const isNum = (v, lim) => typeof v === 'number' && Number.isFinite(v) && Math.abs(v) <= lim;
// Публичный id игрока = короткий хеш его секретного токена (токен хранится в браузере). Так владелец двери/шкафа
// остаётся тем же человеком после перезахода, а подделать чужой id нельзя — секрет не передаётся другим.
function idFromToken(tok) { return 'u' + crypto.createHash('sha256').update('nw|' + tok).digest('hex').slice(0, 10); }
function sendHost(w, obj) { if (w.host && w.host.alive) w.host.send(obj); }

function leaveWorld(c) {
    const w = c.world; if (!w || !c.id) return;
    const wasAttached = c.attached, wasHost = c.isHost;
    w.players.delete(c.id);
    for (const o of w.players.values()) { o.sentVer.delete(c.id); o.sentNick.delete(c.id); }
    c.world = null;
    if (wasHost) {
        w.host = null;
        for (const o of w.players.values()) if (o.attached) { o.attached = false; o.send({ k: 'close' }); } // игроки сами переподключатся, кто-то станет новым хозяином
        persistNow(w);
        console.log('[' + w.id + '] хозяин вышел, игроков осталось ' + w.players.size);
    } else if (wasAttached) {
        sendHost(w, { k: 'leave', pid: c.id });
    }
}

function handleMessage(c, m) {
    if (!c.world) { // первое сообщение — приветствие
        if (m.k !== 'hello') return;
        const w = worlds.get(String(m.world || 'w1'));
        if (!w) { c.send({ k: 'err', type: 'no-world' }); return; }
        if (typeof m.tok !== 'string' || !/^[A-Za-z0-9_-]{16,64}$/.test(m.tok)) { c.send({ k: 'err', type: 'bad-token' }); return; }
        const id = idFromToken(m.tok);
        const old = w.players.get(id);
        if (old) { old.send({ k: 'err', type: 'replaced' }); old.kill(); } // тот же игрок зашёл с другой вкладки/после обрыва — старое соединение закрываем
        if (w.players.size >= MAX_PLAYERS) { c.send({ k: 'err', type: 'full' }); return; }
        if (m.host) {
            if (w.host) { c.send({ k: 'err', type: 'unavailable-id' }); return; } // хозяин уже есть
            w.host = c; c.isHost = true;
        }
        c.world = w; c.id = id; w.players.set(c.id, c);
        c.send({ k: 'open', id: c.id, world: c.isHost ? w.saved : undefined });
        if (c.isHost) console.log('[' + w.id + '] новый хозяин, сохранение: ' + (w.saved ? 'есть' : 'нет'));
        return;
    }
    const w = c.world;
    switch (m.k) {
        case 'connect': { // клиент подключается к хозяину
            if (c.isHost || c.attached) return;
            if (!w.host) { c.send({ k: 'err', type: 'peer-unavailable' }); return; }
            c.attached = true;
            sendHost(w, { k: 'conn', pid: c.id });
            c.send({ k: 'copen' });
            return;
        }
        case 'd': { // данные: игрок -> хозяину, хозяин -> игроку
            if (c.isHost) {
                const o = w.players.get(m.to);
                if (o && o.attached && m.m) o.send({ k: 'd', m: m.m });
            } else if (c.attached && m.m) sendHost(w, { k: 'd', from: c.id, m: m.m });
            return;
        }
        case 'bc': { // хозяин: всем игрокам сразу (кроме одного)
            if (!c.isHost || !m.m) return;
            const frame = textFrame(JSON.stringify({ k: 'd', m: m.m }));
            for (const o of w.players.values()) if (o !== c && o.attached && o.id !== m.except) o.sendFrame(frame);
            return;
        }
        case 's': { // позиция игрока: храним у себя и сами рассылаем соседям
            if (!c.isHost && !c.attached) return;
            if (!isNum(m.x, 2000) || !isNum(m.y, 500) || !isNum(m.z, 2000) || !isNum(m.yaw, 10) || !isNum(m.pitch, 10)) return;
            c.state = { x: m.x, y: m.y, z: m.z, yaw: m.yaw, pitch: m.pitch, item: HELD_ITEMS.includes(m.item) ? m.item : null, air: !!m.air, tp: Number.isInteger(m.tp) ? m.tp : 0 };
            c.nick = typeof m.nick === 'string' ? cleanNick(m.nick) : '';
            c.ver++; c.fwdPending = true;
            return;
        }
        case 'save': { // хозяин присылает сохранение мира
            if (!c.isHost || !m.data || typeof m.data !== 'object') return;
            if (JSON.stringify(m.data).length > MAX_SAVE_BYTES) return;
            w.saved = m.data; schedulePersist(w);
            return;
        }
    }
}

// Раз в тик: рассылаем позиции (близких игроков — часто, далёких — редко) и пересылаем хозяину обновления игроков
let tickNo = 0;
function tick() {
    tickNo++;
    const now = Date.now();
    for (const w of worlds.values()) {
        const list = [];
        for (const p of w.players.values()) if (p.state && (p.attached || p.isHost)) list.push(p);
        if (!list.length) continue;
        // 1) хозяину — состояние игроков не чаще 4 раз в секунду (ему нужно для проверки ударов и таймаутов)
        if (w.host) for (const p of list) {
            if (p.isHost || !p.fwdPending) continue;
            if (now - p.lastFwd >= (p.lastFwd === 0 ? 0 : 250)) {
                p.lastFwd = now; p.fwdPending = false;
                w.host.send({ k: 'd', from: p.id, m: { t: 's', ...p.state, nick: p.nick } });
            }
        }
        // 2) всем — позиции других игроков
        for (const r of list) {
            if (!r.alive) continue;
            const out = [];
            for (const o of list) {
                if (o === r) continue;
                const first = !r.sentVer.has(o.id);
                if (!first && r.sentVer.get(o.id) === o.ver) continue;
                if (!first) {
                    const d = Math.hypot(r.state.x - o.state.x, r.state.z - o.state.z);
                    const period = d < 30 ? 2 : d < 100 ? 5 : 20; // рядом ~10 раз/с, подальше ~4, совсем далеко ~1
                    if (tickNo % period !== 0) continue;
                }
                const st = o.state; // компактная запись: [id, x, y, z, yaw, pitch, предмет(номер), в воздухе, метка возрождения, ник (только если изменился)]
                const e = [o.id, st.x, st.y, st.z, st.yaw, st.pitch, st.item ? HELD_ITEMS.indexOf(st.item) + 1 : 0, st.air ? 1 : 0, st.tp];
                if (r.sentNick.get(o.id) !== o.nick) { e.push(o.nick); r.sentNick.set(o.id, o.nick); }
                r.sentVer.set(o.id, o.ver);
                out.push(e);
            }
            if (out.length) r.send({ k: 'snap', p: out });
        }
    }
}
setInterval(tick, TICK_MS);

// Проверка, что игрок ещё жив (браузер отвечает на ping сам)
const allConns = new Set();
setInterval(() => {
    const now = Date.now();
    for (const c of allConns) {
        if (!c.alive) { allConns.delete(c); continue; }
        if (now - c.lastPong > 70000) { c.kill(); continue; }
        c.sendFrame(encodeFrame(0x9, Buffer.alloc(0)));
    }
}, 25000);

// ---------------------------------------------------------------------
// HTTP: список миров, проверка работы, при желании — сама игра (index.html рядом с server.js)
// ---------------------------------------------------------------------
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS' };
const server = http.createServer((req, res) => {
    const url = (req.url || '/').split('?')[0];
    if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }
    if (url === '/worlds') {
        const list = [...worlds.values()].map(w => ({ id: w.id, name: w.name, players: [...w.players.values()].filter(p => p.attached || p.isHost).length, max: MAX_PLAYERS }));
        res.writeHead(200, { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end(JSON.stringify(list)); return;
    }
    if (url === '/health') { res.writeHead(200, CORS); res.end('ok'); return; }
    if (url === '/' || url === '/index.html') {
        const f = [path.join(__dirname, 'index.html'), path.join(__dirname, 'public', 'index.html')].find(x => fs.existsSync(x));
        if (f) { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); fs.createReadStream(f).pipe(res); return; }
        res.writeHead(200, { ...CORS, 'Content-Type': 'text/plain; charset=utf-8' }); res.end('Nature Walk server: работает\n'); return;
    }
    res.writeHead(404, CORS); res.end('not found');
});
server.on('upgrade', (req, socket) => {
    const key = req.headers['sec-websocket-key'];
    if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
    const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
    socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
    allConns.add(new Conn(socket));
});

setInterval(() => {
    console.log([...worlds.values()].map(w => w.id + ':' + w.players.size).join(' ') + ' · память ' + Math.round(process.memoryUsage().rss / 1048576) + ' МБ');
}, 60000);

async function main() {
    for (const w of worlds.values()) {
        w.saved = await store.load(w.id);
        console.log('[' + w.id + '] ' + w.name + ': сохранение ' + (w.saved ? 'загружено' : 'нет'));
    }
    server.listen(PORT, () => console.log('Игровой сервер запущен на порту ' + PORT + ' (миров: ' + worlds.size + ', до ' + MAX_PLAYERS + ' игроков в каждом)'));
}
main();

async function shutdown() {
    console.log('Остановка: сохраняю миры…');
    for (const w of worlds.values()) { try { await persistNow(w); } catch (_) {} }
    process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

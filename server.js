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
const SAVE_DELAY_MS = 20000;      // как часто пишем мир в хранилище (бесплатная база Upstash ограничена по числу запросов, поэтому не чаще)
const PLAYER_SAVE_DELAY_MS = 20000;
const URGENT_MIN_GAP_MS = 5000;   // «срочное» сохранение (игрок вышел, умер, построил) — но не чаще раза в 5 секунд на мир
const PLAYER_KEEP_MS = 30 * 24 * 3600 * 1000; // данные игрока, который не заходил 30 дней, стираем
const MAX_PLAYER_RECORDS = 3000;
const HELD_ITEMS = ['stone', 'pickaxe', 'axe', 'mallet', 'torch', 'chertyozh', 'spear'];

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ---------------------------------------------------------------------
// Хранилище сохранений: файл (если диск не стирается) и/или Upstash Redis (бесплатно, переживает перезапуски Render)
// ---------------------------------------------------------------------
const store = {
    async load(id) {
        if (UPSTASH_URL && UPSTASH_TOKEN) {
            // Ошибку базы НЕЛЬЗЯ принимать за «данных нет»: иначе пустой список перезапишет настоящие аккаунты и мир. Поэтому пробуем несколько раз, а потом сообщаем об ошибке.
            let lastErr = null;
            for (let i = 0; i < 4; i++) {
                try {
                    const r = await fetch(UPSTASH_URL, { method: 'POST', headers: { Authorization: 'Bearer ' + UPSTASH_TOKEN }, body: JSON.stringify(['GET', 'nw:' + id]) });
                    if (!r.ok) throw new Error('HTTP ' + r.status);
                    const j = await r.json();
                    if (j && typeof j.result === 'string') return JSON.parse(j.result);
                    if (j && j.result === null) { lastErr = null; break; } // ключа нет — это нормально (первый запуск)
                    throw new Error('неожиданный ответ базы');
                } catch (e) { lastErr = e; console.error('load upstash', id, e.message); await new Promise(r => setTimeout(r, 1000 * (i + 1))); }
            }
            if (lastErr) throw lastErr;
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
WORLD_NAMES.forEach((name, i) => worlds.set('w' + (i + 1), { id: 'w' + (i + 1), name, host: null, players: new Map(), saved: null, saveTimer: null, dirty: false, pdata: {}, pTimer: null, pDirty: false }));

function schedulePersist(w, urgent) {
    w.dirty = true;
    const now = Date.now();
    if (urgent) { // после важного действия сохраняем почти сразу, но не чаще раза в 5 секунд
        const wait = Math.max(300, (w.lastUrgent || 0) + URGENT_MIN_GAP_MS - now);
        if (w.saveTimer) clearTimeout(w.saveTimer);
        w.lastUrgent = now + wait;
        w.saveTimer = setTimeout(() => { w.saveTimer = null; persistNow(w); }, wait);
        return;
    }
    if (w.saveTimer) return;
    w.saveTimer = setTimeout(() => { w.saveTimer = null; persistNow(w); }, SAVE_DELAY_MS);
}
async function persistNow(w) {
    if (w.dirty && w.saved) { w.dirty = false; await store.save(w.id, w.saved); }
    await persistPlayers(w);
}
// Данные игроков (инвентарь и место, где вышел) лежат отдельно от мира: id игрока -> запись. Хозяин мира их не трогает.
function schedulePersistPlayers(w, urgent) {
    w.pDirty = true;
    const now = Date.now();
    if (urgent) {
        const wait = Math.max(300, (w.pLastUrgent || 0) + URGENT_MIN_GAP_MS - now);
        if (w.pTimer) clearTimeout(w.pTimer);
        w.pLastUrgent = now + wait;
        w.pTimer = setTimeout(() => { w.pTimer = null; persistPlayers(w); }, wait);
        return;
    }
    if (w.pTimer) return;
    w.pTimer = setTimeout(() => { w.pTimer = null; persistPlayers(w); }, PLAYER_SAVE_DELAY_MS);
}
async function persistPlayers(w) {
    if (!w.pDirty) return;
    w.pDirty = false;
    await store.save(w.id + '_p', { v: 1, players: w.pdata });
}
// Проверка записи от игрока: только числа и простые названия предметов, ничего лишнего
const ITEM_TYPE = /^[a-z0-9_]{1,32}$/;
function cleanPlayerRec(m, nick) {
    if (!Array.isArray(m.inv) || m.inv.length > 40) return null;
    const inv = m.inv.map(it => (it && typeof it.type === 'string' && ITEM_TYPE.test(it.type) && Number.isInteger(it.count) && it.count >= 1 && it.count <= 999) ? { type: it.type, count: it.count } : null);
    if (!isNum(m.x, 2000) || !isNum(m.y, 500) || !isNum(m.z, 2000) || !isNum(m.yaw, 10) || !isNum(m.pitch, 10)) return null;
    return { inv, sel: Number.isInteger(m.sel) && m.sel >= 0 && m.sel < 40 ? m.sel : 0, dead: !!m.dead, x: m.x, y: m.y, z: m.z, yaw: m.yaw, pitch: m.pitch, nick: nick || '', at: Date.now() };
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
    const prec = w.pdata[c.id]; // запоминаем, где именно игрок вышел (по последней позиции, которую знает сервер)
    if (prec && c.pSynced) {
        if (!prec.dead && c.state) { prec.x = c.state.x; prec.y = c.state.y; prec.z = c.state.z; prec.yaw = c.state.yaw; prec.pitch = c.state.pitch; }
        if (c.nick) prec.nick = c.nick;
        prec.at = Date.now(); schedulePersistPlayers(w, true); // вышел — запоминаем срочно
    }
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
        if (!storageReady) { c.send({ k: 'err', type: 'network' }); return; } // данные ещё грузятся — клиент подождёт и переподключится
        const w = worlds.get(String(m.world || 'w1'));
        if (!w) { c.send({ k: 'err', type: 'no-world' }); return; }
        if (typeof m.tok !== 'string' || !/^[A-Za-z0-9_-]{16,64}$/.test(m.tok)) { c.send({ k: 'err', type: 'bad-token' }); return; }
        const acc = accByTok.get(m.tok);
        if (!acc) { c.send({ k: 'err', type: 'no-account' }); return; } // заходить можно только с аккаунтом
        const id = idFromToken(m.tok);
        const old = w.players.get(id);
        if (old) { old.send({ k: 'err', type: 'replaced' }); old.kill(); } // тот же игрок зашёл с другой вкладки/после обрыва — старое соединение закрываем
        if (w.players.size >= MAX_PLAYERS) { c.send({ k: 'err', type: 'full' }); return; }
        if (m.host) {
            if (w.host) { c.send({ k: 'err', type: 'unavailable-id' }); return; } // хозяин уже есть
            w.host = c; c.isHost = true;
        }
        c.world = w; c.id = id; c.accName = acc.name; c.admin = acc.admin === true; w.players.set(c.id, c);
        c.send({ k: 'open', id: c.id, world: c.isHost ? w.saved : undefined, pdata: w.pdata[c.id] || undefined, admin: c.admin || undefined });
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
            c.nick = c.accName || (typeof m.nick === 'string' ? cleanNick(m.nick) : ''); // ник = имя аккаунта, подделать нельзя
            c.ver++; c.fwdPending = true;
            return;
        }
        case 'pinv': { // игрок присылает свой инвентарь и положение — сервер хранит их до следующего захода
            if (!c.isHost && !c.attached) return;
            const rec = cleanPlayerRec(m, c.nick);
            if (!rec) return;
            c.pSynced = true;
            if (!w.pdata[c.id] && Object.keys(w.pdata).length >= MAX_PLAYER_RECORDS) return;
            w.pdata[c.id] = rec; schedulePersistPlayers(w, m.u === 1);
            return;
        }
        case 'save': { // хозяин присылает сохранение мира
            if (!c.isHost || !m.data || typeof m.data !== 'object') return;
            if (JSON.stringify(m.data).length > MAX_SAVE_BYTES) return;
            w.saved = m.data; schedulePersist(w, m.u === 1);
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
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type' };

// ---------------------------------------------------------------------
// Аккаунты: имя + пароль. У аккаунта есть секретный токен — им игрок заходит в мир с любого устройства (id игрока считается от токена,
// поэтому постройки, двери, инвентарь и место выхода «идут» за аккаунтом). Пароль храним только в виде хеша (scrypt + соль).
// ---------------------------------------------------------------------
// Ключ имени для проверки «такое уже есть»: без регистра, пробелов, _ - . и невидимых символов, а русские буквы-двойники
// (а, е, о, р, с, у, х, к, м, т, в, н) приравнены к латинским. Поэтому «Вася», «вася», «В а с я» и «Bася» — одно и то же имя.
const HOMO = { 'а': 'a', 'в': 'b', 'е': 'e', 'ё': 'e', 'з': '3', 'к': 'k', 'м': 'm', 'н': 'h', 'о': 'o', 'р': 'p', 'с': 'c', 'т': 't', 'у': 'y', 'х': 'x', 'і': 'i', 'ј': 'j', 'ѕ': 's' };
function nameKey(n) {
    const s = String(n).normalize('NFKC').toLowerCase().replace(/[\u200b-\u200f\u2060\ufeff\u00ad]/g, '').replace(/[\s._-]+/g, '');
    return Array.from(s).map(ch => HOMO[ch] || ch).join('');
}
const accByName = new Map(); // ключ имени (nameKey) -> { name, salt, hash, tok, created }
const accByTok = new Map();
async function loadAccounts() {
    const d = await store.load('accounts');
    accByName.clear(); accByTok.clear();
    if (d && Array.isArray(d.list)) for (const a of d.list) if (a && typeof a.name === 'string' && typeof a.tok === 'string') { const k = nameKey(a.name); if (accByName.has(k)) console.warn('Двойник имени в базе: «' + a.name + '» совпадает с «' + accByName.get(k).name + '» — войти по имени можно только в первый, второй пока работает по ключу'); else accByName.set(k, a); accByTok.set(a.tok, a); }
    console.log('Аккаунтов: ' + accByName.size);
}
let accSaving = Promise.resolve();
function saveAccounts() {
    accSaving = accSaving.then(() => store.save('accounts', { v: 1, list: [...accByTok.values()] })).catch(e => console.error('save accounts', e.message));
    return accSaving;
}
// Главный админ: аккаунт создаётся/обновляется при каждом запуске сервера (хранится только хеш пароля, не сам пароль).
// Имя занято навсегда — зарегистрировать такое же никто не сможет.
const ADMIN_NAME = 'cap_ukapka', ADMIN_SALT = 'e51aaa991235e1933769493c7d10800f', ADMIN_HASH = '208bf4394e2921e18c349a758a002cfe68c2e978d05924600cc16d76050e0743';
async function seedAdmin() {
    const key = nameKey(ADMIN_NAME);
    let a = accByName.get(key), changed = false;
    if (!a) { a = { name: ADMIN_NAME, salt: ADMIN_SALT, hash: ADMIN_HASH, tok: crypto.randomBytes(20).toString('hex'), created: Date.now(), admin: true }; accByName.set(key, a); accByTok.set(a.tok, a); changed = true; }
    else if (a.salt !== ADMIN_SALT || a.hash !== ADMIN_HASH || a.admin !== true || a.name !== ADMIN_NAME) { a.salt = ADMIN_SALT; a.hash = ADMIN_HASH; a.admin = true; a.name = ADMIN_NAME; changed = true; }
    if (changed) await saveAccounts();
}
const hashPass = (pass, salt) => crypto.scryptSync(pass, salt, 32).toString('hex');
const rlHits = new Map(); // защита от подбора пароля и спама аккаунтами: ключ -> времена попыток
function rlCount(key, windowMs) { const now = Date.now(); const a = (rlHits.get(key) || []).filter(t => now - t < windowMs); rlHits.set(key, a); return a.length; }
function rlHit(key) { const a = rlHits.get(key) || []; a.push(Date.now()); rlHits.set(key, a); }
setInterval(() => { const now = Date.now(); for (const [k, a] of rlHits) if (!a.length || now - a[a.length - 1] > 3600000) rlHits.delete(k); }, 600000);
function clientIp(req) { return String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress || '?'; }
function sendJson(res, code, obj) { res.writeHead(code, { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); }
function handleAuth(req, res, kind) {
    let body = '', big = false;
    req.on('data', (c) => { body += c; if (body.length > 4096) { big = true; req.destroy(); } });
    req.on('end', async () => {
        if (big) return;
        if (!storageReady) return sendJson(res, 503, { err: 'server' });
        try {
            let b; try { b = JSON.parse(body); } catch (_) { return sendJson(res, 400, { err: 'bad' }); }
            if (!b || typeof b !== 'object') return sendJson(res, 400, { err: 'bad' });
            const ip = clientIp(req);
            const raw = typeof b.name === 'string' ? b.name.trim() : '';
            const name = cleanNick(raw), pass = typeof b.pass === 'string' ? b.pass : '';
            if (name.length < 3 || Array.from(raw).length > 15 || name !== raw.replace(/\s+/g, ' ')) return sendJson(res, 400, { err: 'name' });
            if (pass.length < 4 || pass.length > 64) return sendJson(res, 400, { err: 'pass' });
            const key = nameKey(name);
            if (kind === 'register') {
                if (rlCount('r' + ip, 3600000) >= 5) return sendJson(res, 429, { err: 'rate' });
                const ex = accByName.get(key);
                if (ex) { // имя занято: если пароль тот же — это просто вход в свой аккаунт, иначе отказ
                    const h = Buffer.from(hashPass(pass, ex.salt), 'hex'), a = Buffer.from(ex.hash, 'hex');
                    if (h.length === a.length && crypto.timingSafeEqual(h, a)) return sendJson(res, 200, { ok: true, name: ex.name, tok: ex.tok });
                    return sendJson(res, 409, { err: 'taken' });
                }
                // токен этого устройства (если ещё ничей) становится токеном аккаунта — тогда прежние постройки и вещи останутся за игроком
                const tok = (typeof b.tok === 'string' && /^[A-Za-z0-9_-]{16,64}$/.test(b.tok) && !accByTok.has(b.tok)) ? b.tok : crypto.randomBytes(20).toString('hex');
                const salt = crypto.randomBytes(16).toString('hex');
                const acc = { name, salt, hash: hashPass(pass, salt), tok, created: Date.now() };
                accByName.set(key, acc); accByTok.set(tok, acc);
                rlHit('r' + ip);
                await saveAccounts();
                console.log('Новый аккаунт: ' + name);
                return sendJson(res, 200, { ok: true, name, tok });
            }
            if (rlCount('f' + ip, 600000) >= 8) return sendJson(res, 429, { err: 'rate' });
            const acc = accByName.get(key);
            let ok = false;
            if (acc) { const h = Buffer.from(hashPass(pass, acc.salt), 'hex'), a = Buffer.from(acc.hash, 'hex'); ok = h.length === a.length && crypto.timingSafeEqual(h, a); }
            if (!ok) { rlHit('f' + ip); return sendJson(res, 401, { err: 'bad' }); }
            return sendJson(res, 200, { ok: true, name: acc.name, tok: acc.tok });
        } catch (e) { console.error('auth', e); sendJson(res, 500, { err: 'server' }); }
    });
}

const server = http.createServer((req, res) => {
    const url = (req.url || '/').split('?')[0];
    if (req.method === 'OPTIONS') { res.writeHead(204, CORS); res.end(); return; }
    if (url === '/worlds') {
        const list = [...worlds.values()].map(w => ({ id: w.id, name: w.name, players: [...w.players.values()].filter(p => p.attached || p.isHost).length, max: MAX_PLAYERS }));
        res.writeHead(200, { ...CORS, 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
        res.end(JSON.stringify(list)); return;
    }
    if (req.method === 'POST' && (url === '/register' || url === '/login')) { handleAuth(req, res, url.slice(1)); return; }
    if (url === '/health') { // по этому адресу видно, где сервер хранит данные
        res.writeHead(200, { ...CORS, 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('ok\nготов: ' + (storageReady ? 'да' : 'нет, хранилище ещё не загрузилось') + '\nхранилище: ' + ((UPSTASH_URL && UPSTASH_TOKEN) ? 'Upstash (данные переживут перезапуск)' : 'ТОЛЬКО ФАЙЛ — при перезапуске Render данные и аккаунты СТИРАЮТСЯ') + '\nаккаунтов: ' + accByName.size + '\nработает без перезапуска: ' + Math.round(process.uptime() / 60) + ' мин (если число маленькое — сервер недавно перезапускался)\nсейчас подключено: ' + allConns.size);
        return;
    }
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

// Бесплатный Render усыпляет сервер после 15 минут без запросов, а просыпается он до минуты — отсюда «переподключение».
// Поэтому раз в 10 минут сервер сам стучится на свой публичный адрес (Render сам подставляет RENDER_EXTERNAL_URL) и не засыпает.
const SELF_URL = (process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
if (SELF_URL) setInterval(() => { fetch(SELF_URL + '/health').catch(() => {}); }, 10 * 60 * 1000);

// Пока данные не загружены из хранилища (или оно временно не отвечает), сервер никого не пускает и ничего не сохраняет — так аккаунты и мир не затираются пустыми данными
let storageReady = false, loadingNow = false;
async function loadAll() {
    if (storageReady || loadingNow) return;
    loadingNow = true;
    try {
        await loadAccounts(); await seedAdmin();
        for (const w of worlds.values()) {
            w.saved = await store.load(w.id);
            w.pdata = {};
            const pl = await store.load(w.id + '_p');
            if (pl && pl.players && typeof pl.players === 'object') {
                const now = Date.now();
                for (const [id, r] of Object.entries(pl.players)) if (r && typeof r.at === 'number' && now - r.at < PLAYER_KEEP_MS) w.pdata[id] = r;
            }
            console.log('[' + w.id + '] ' + w.name + ': сохранение ' + (w.saved ? 'загружено' : 'нет') + ', игроков в базе: ' + Object.keys(w.pdata).length);
        }
        storageReady = true;
        console.log('Хранилище загружено, сервер принимает игроков');
    } catch (e) {
        console.error('Хранилище не загрузилось (' + e.message + '), повторю через 10 секунд; игроков пока не пускаю');
        setTimeout(loadAll, 10000);
    }
    loadingNow = false;
}

async function main() {
    if (!(UPSTASH_URL && UPSTASH_TOKEN)) console.warn('ВНИМАНИЕ: Upstash не подключён — аккаунты и мир хранятся в файле и пропадут при перезапуске сервера на Render!');
    await Promise.race([loadAll(), new Promise(r => setTimeout(r, 15000))]); // Render ждёт открытый порт — если база отвечает долго, открываем порт и догружаем в фоне
    server.listen(PORT, () => console.log('Игровой сервер запущен на порту ' + PORT + ' (миров: ' + worlds.size + ', до ' + MAX_PLAYERS + ' игроков в каждом)'));
}
main();

let shuttingDown = false;
async function shutdown() {
    if (shuttingDown) return; shuttingDown = true;
    console.log('Остановка: сохраняю миры…');
    setTimeout(() => process.exit(0), 8000).unref(); // Render даёт мало времени — не зависаем на медленной базе
    if (storageReady) await Promise.all([...worlds.values()].map(async w => { try { if (w.dirty && w.saved) { w.dirty = false; await store.save(w.id, w.saved); } w.pDirty = true; await persistPlayers(w); } catch (_) {} }));
    try { await saveAccounts(); } catch (_) {}
    process.exit(0);
}
process.on('uncaughtException', (e) => { console.error('uncaughtException (сервер продолжает работать):', e && e.stack || e); }); // одна ошибка не должна ронять весь сервер и стирать несохранённое
process.on('unhandledRejection', (e) => { console.error('unhandledRejection:', e && e.stack || e); });
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

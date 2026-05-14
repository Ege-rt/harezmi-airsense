require("dotenv").config({ path: require("path").resolve(__dirname, ".env") });

const dns = require("dns");
dns.setDefaultResultOrder("ipv4first");
dns.setServers(["8.8.8.8", "8.8.4.4"]);

const express = require("express");
const cors = require("cors");
const path = require("path");
const { MongoClient } = require("mongodb");

const app = express();
const PORT = process.env.PORT || 3000;

// ─────────────────────────────────────────────
// ENV SAFETY CHECK (CRITICAL)
// ─────────────────────────────────────────────
if (!process.env.MONGO_URI) {
    console.error("❌ MONGO_URI missing in .env");
    process.exit(1);
}

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = "harezmi_iot";

console.log("DB:", DB_NAME);

// ─────────────────────────────────────────────
// GLOBAL STATE
// ─────────────────────────────────────────────
let client;
let db;
let sensorCollection;
let historyCollection;
let fanStatesCollection;

let isConnected = false;
let isConnecting = false;
let indexesCreated = false;

const MAX_RETRY = 10;
let retryCount = 0;

// ─────────────────────────────────────────────
// INDEXES
// ─────────────────────────────────────────────
async function createIndexes() {
    if (indexesCreated) return;

    await sensorCollection.createIndex({ deviceId: 1 }, { unique: true });
    await historyCollection.createIndex({ timestamp: -1 });
    await historyCollection.createIndex(
        { timestamp: 1 },
        { expireAfterSeconds: 60 * 60 * 24 * 30 }
    );

    indexesCreated = true;
    console.log("📊 Indexes created once");
}

// ─────────────────────────────────────────────
// MONGO CONNECT (SAFE + CONTROLLED RETRY)
// ─────────────────────────────────────────────
async function connectMongo() {
    if (isConnected) return db;
    if (isConnecting) return;

    isConnecting = true;

    try {
        console.log("🔄 Connecting to MongoDB...");

        if (!client) {
            client = new MongoClient(MONGO_URI, {
                maxPoolSize: 20,
                minPoolSize: 5,
                serverSelectionTimeoutMS: 15000,
                socketTimeoutMS: 30000,
                connectTimeoutMS: 15000,
                retryWrites: true,
                family: 4
            });
        }

        await client.connect();

        db = client.db(DB_NAME);
        sensorCollection = db.collection("sensors");
        historyCollection   = db.collection("history");
        fanStatesCollection = db.collection("fan_states");

        await createIndexes();

        isConnected = true;
        isConnecting = false;
        retryCount = 0;

        console.log("✅ MongoDB Connected Successfully");

        client.on("serverOpen", () => console.log("🟢 MongoDB server open"));

        client.on("serverClose", () => {
            console.log("🔴 MongoDB server closed");
            isConnected = false;
        });

        client.on("error", (err) => {
            console.log("⚠️ MongoDB error:", err.message);
            isConnected = false;
        });

        return db;

    } catch (err) {
        console.error("❌ MongoDB Connection Error:", err.message);

        isConnected = false;
        isConnecting = false;

        retryCount++;

        if (retryCount <= MAX_RETRY) {
            console.log(`🔁 Retry ${retryCount}/${MAX_RETRY} in 5s...`);

            setTimeout(() => {
                connectMongo();
            }, 5000);

        } else {
            console.log("⛔ Max retry limit reached. Exiting...");
        }
    }
}

// ─────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────
async function closeMongo() {
    try {
        if (client) {
            await client.close();
            console.log("🛑 MongoDB connection closed");
        }
    } catch (err) {
        console.log("Close error:", err.message);
    }
}

// ─────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────
app.use(cors({ origin: "*" }));
app.use(express.json());
app.use(express.static(path.join(__dirname)));

// ─────────────────────────────────────────────
// RAM CACHE
// ─────────────────────────────────────────────
let sensorStore = {};
const OFFLINE_THRESHOLD_MS = 30 * 1000;

// Fan komutları
let fanCommands = {};

// ─────────────────────────────────────────────
// VALIDATION
// ─────────────────────────────────────────────
function validate(data) {
    const errors = [];

    if (!data.deviceId) errors.push("deviceId required");

    if (data.temperature && isNaN(data.temperature))
        errors.push("temperature invalid");

    if (data.pm25 && isNaN(data.pm25))
        errors.push("pm25 invalid");

    return errors;
}

// ─────────────────────────────────────────────
// API: POST SENSOR DATA
// ─────────────────────────────────────────────
app.post("/api/sensors", async (req, res) => {
    const data = req.body;

    const errors = validate(data);
    if (errors.length) {
        return res.status(400).json({ error: errors });
    }

    const deviceId = data.deviceId;
    const now = new Date();

    const entry = {
        deviceId,
        temperature: Number(data.temperature) || null,
        humidity: Number(data.humidity) || null,
        pm25: Number(data.pm25) || null,
        gas: Number(data.gas) || null,
        fanState: !!data.fanState,
        class: data.class || null,
        lastSeen: now,
        online: true
    };

    sensorStore[deviceId] = entry;

    try {
        await sensorCollection.updateOne(
            { deviceId },
            { $set: entry },
            { upsert: true }
        );

        await historyCollection.insertOne({
            ...entry,
            timestamp: now
        });

        console.log("📡 Data saved:", deviceId);

        res.json({ success: true });

    } catch (err) {
        console.error("DB write error:", err.message);
        res.status(500).json({ error: "DB error" });
    }
});

// ─────────────────────────────────────────────
// API: FAN COMMAND (WEB → ESP)
// ─────────────────────────────────────────────
app.post("/api/fan-command", (req, res) => {
    const { deviceId, command } = req.body;
    if (!deviceId) return res.status(400).json({ error: "deviceId required" });

    if (command === null || command === undefined) {
        delete fanCommands[deviceId];  // ← Override'ı tamamen sil
        console.log(`🌀 Fan override iptal: ${deviceId}`);
    } else {
        fanCommands[deviceId] = command;
        console.log(`🌀 Fan komutu: ${deviceId} → ${command ? "AÇIK" : "KAPALI"}`);
    }

    res.json({ success: true });
});

app.get("/api/fan-command", (req, res) => {
    const { deviceId } = req.query;
    if (!deviceId) return res.status(400).json({ error: "deviceId required" });
    res.json({ fanOverride: fanCommands[deviceId] ?? null });
});

// ─────────────────────────────────────────────
// API: FAN STATES (Manuel açma/kapama kalıcı kayıt)
// GET  /api/fan-states       → Tüm fan durumlarını getir
// POST /api/fan-states       → Fan durumlarını kaydet (upsert)
// ─────────────────────────────────────────────
app.get("/api/fan-states", async (req, res) => {
    try {
        const doc = await fanStatesCollection.findOne({ _id: "global" });
        if (!doc) return res.json({ states: {}, savedAt: null });
        res.json({ states: doc.states || {}, savedAt: doc.savedAt || null });
    } catch (err) {
        console.error("fan-states GET hatası:", err.message);
        res.status(500).json({ error: "DB error" });
    }
});

app.post("/api/fan-states", async (req, res) => {
    const { states, savedAt } = req.body;
    if (!states || typeof states !== "object") {
        return res.status(400).json({ error: "Geçersiz veri" });
    }
    try {
        await fanStatesCollection.updateOne(
            { _id: "global" },
            { $set: { states, savedAt: savedAt || new Date().toISOString() } },
            { upsert: true }
        );
        console.log("💾 Fan durumları kaydedildi:", Object.keys(states).length, "sınıf");
        res.json({ success: true });
    } catch (err) {
        console.error("fan-states POST hatası:", err.message);
        res.status(500).json({ error: "DB error" });
    }
});

// ─────────────────────────────────────────────
// API: GET LIVE DATA
// ─────────────────────────────────────────────
app.get("/api/sensors", (req, res) => {
    const now = Date.now();

    const result = Object.values(sensorStore).map(s => ({
        ...s,
        online: (now - new Date(s.lastSeen).getTime()) < OFFLINE_THRESHOLD_MS
    }));

    res.json(result);
});

// ─────────────────────────────────────────────
// API: HISTORY - Günlük ortalama (cihazdan cihaza erişim)
// GET /api/history/daily?days=30
// ─────────────────────────────────────────────
app.get("/api/history/daily", async (req, res) => {
    const days = Math.min(parseInt(req.query.days) || 30, 90);

    try {
        const since = new Date();
        since.setDate(since.getDate() - days);

        const records = await historyCollection.find(
            { timestamp: { $gte: since } },
            { projection: { _id: 0, deviceId: 1, class: 1, temperature: 1, humidity: 1, pm25: 1, gas: 1, timestamp: 1 } }
        ).sort({ timestamp: 1 }).toArray();

        // Günlük + sınıf bazında grupla
        const grouped = {}; // { "2025-06-10": { "9-A": { pmSum, gasSum, tSum, count, scoreSum } } }

        records.forEach(r => {
            if (!r.class || r.pm25 == null) return;

            const dk = new Date(r.timestamp).toISOString().slice(0, 10);
            const className = r.class;

            if (!grouped[dk]) grouped[dk] = {};
            if (!grouped[dk][className]) grouped[dk][className] = { pmSum: 0, gasSum: 0, tSum: 0, count: 0, scoreSum: 0 };

            const entry = grouped[dk][className];
            const pm  = Number(r.pm25)      || 0;
            const gas = Number(r.gas)        || 0;
            const t   = Number(r.temperature)|| 0;

            // Skor hesapla (frontend ile aynı formül)
            let score = 100;
            if      (pm <= 12)  score -= 0;
            else if (pm <= 25)  score -= (pm - 12) * 2;
            else if (pm <= 50)  score -= 26 + (pm - 25) * 1.2;
            else                score -= 56;
            score -= Math.min(35, (gas / 1000) * 35);
            if      (t > 28) score -= Math.min(15, (t - 28) * 2);
            else if (t < 18) score -= Math.min(10, (18 - t) * 1);
            score = Math.min(100, Math.max(1, Math.floor(score)));

            entry.pmSum    += pm;
            entry.gasSum   += gas;
            entry.tSum     += t;
            entry.scoreSum += score;
            entry.count    += 1;
        });

        // Frontend'in beklediği formata çevir
        const result = {};
        Object.entries(grouped).forEach(([dk, classes]) => {
            result[dk] = {};
            Object.entries(classes).forEach(([name, r]) => {
                result[dk][name] = {
                    pmSum:    r.pmSum,
                    gasSum:   r.gasSum,
                    tSum:     r.tSum,
                    scoreSum: r.scoreSum,
                    count:    r.count
                };
            });
        });

        res.json(result);

    } catch (err) {
        console.error("History daily error:", err.message);
        res.status(500).json({ error: "DB error" });
    }
});

// ─────────────────────────────────────────────
// API: STATUS
// ─────────────────────────────────────────────
app.get("/api/status", async (req, res) => {
    const historyCount = await historyCollection
        ?.estimatedDocumentCount()
        .catch(() => 0);

    res.json({
        status: "ok",
        mongo: !!db,
        devices: Object.keys(sensorStore).length,
        history: historyCount,
        uptime: process.uptime()
    });
});

// ─────────────────────────────────────────────
// ROOT
// ─────────────────────────────────────────────
app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "index.html"));
});

// ─────────────────────────────────────────────
// START SERVER (SAFE BOOT)
// ─────────────────────────────────────────────
async function start() {
    await connectMongo();

    if (!db) {
        console.error("❌ MongoDB not connected. Server aborted.");
        process.exit(1);
    }

    app.listen(PORT, () => {
        console.log(`🚀 Server running → http://localhost:${PORT}`);
    });
}

start();

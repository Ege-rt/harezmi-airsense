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
        historyCollection = db.collection("history");

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

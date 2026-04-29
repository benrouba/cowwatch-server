// server.js
// CowWatch Backend — Render.com Free Hosting
// Handles: ESP32 data → Firebase RTDB + FCM v1 push notification
//
// HOW IT WORKS:
//   ESP32 sends HTTP POST to this server (instead of Firebase directly)
//   This server:
//     1. Writes sensor data to Firebase Realtime Database
//     2. Sends push notification via FCM v1 API (new, non-deprecated)
//
// DEPLOY: render.com (free) — see REAL_DATA_GUIDE.md Step 3

const express      = require('express');
const admin        = require('firebase-admin');
const cors         = require('cors');

const app  = express();
app.use(express.json());
app.use(cors());

// ── Firebase Admin SDK init ──────────────────────────────
// Uses service account JSON (set as environment variable on Render)
let db;
try {
  const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
  admin.initializeApp({
    credential:  admin.credential.cert(serviceAccount),
    databaseURL: process.env.FIREBASE_DATABASE_URL,
  });
  db = admin.database();
  console.log('[Firebase] Admin SDK initialized OK');
} catch (e) {
  console.error('[Firebase] Init failed:', e.message);
}

// ════════════════════════════════════════════════════════
//  POST /sensor
//  Called by ESP32 every 5 seconds with live sensor data
//  Body: { cowId, cowName, farmerId, temperature,
//          activity, heatScore, status }
// ════════════════════════════════════════════════════════
app.post('/sensor', async (req, res) => {
  const { cowId, cowName, farmerId,
          temperature, activity, heatScore, status } = req.body;

  if (!cowId) return res.status(400).json({ error: 'Missing cowId' });

  try {
    // 1. Write live data to Firebase RTDB
    const path    = `/cows/${cowId}/latest`;
    const payload = {
      cowId, cowName, farmerId,
      temperature, activity, heatScore, status,
      timestamp: admin.database.ServerValue.TIMESTAMP,
    };
    await db.ref(path).set(payload);

    // 2. Also push to history (last 100 entries)
    await db.ref(`/cows/${cowId}/history`).push(payload);

    console.log(`[Sensor] ${cowId} | ${temperature}°C | ${status}`);
    res.json({ ok: true });

  } catch (e) {
    console.error('[Sensor] DB write failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ════════════════════════════════════════════════════════
//  POST /alert
//  Called by ESP32 when heat score crosses 65%
//  Body: { cowId, cowName, farmerId,
//          temperature, activity, heatScore }
// ════════════════════════════════════════════════════════
app.post('/alert', async (req, res) => {
  const { cowId, cowName, farmerId,
          temperature, activity, heatScore } = req.body;

  if (!cowId) return res.status(400).json({ error: 'Missing cowId' });

  try {
    // 1. Get farmer's FCM token from Firebase
    const tokenSnap = await db.ref(`/farmers/${farmerId}/fcmToken`).get();
    const fcmToken  = tokenSnap.val();

    if (!fcmToken) {
      console.log(`[Alert] No FCM token for farmer ${farmerId} — skipping`);
      return res.json({ ok: true, sent: false, reason: 'No FCM token' });
    }

    // 2. Write alert to Firebase
    await db.ref(`/alerts/${cowId}`).set({
      cowId, cowName, farmerId,
      temperature, activity, heatScore,
      notified:  true,
      timestamp: admin.database.ServerValue.TIMESTAMP,
    });

    // 3. Send FCM v1 push notification
    //    firebase-admin uses FCM v1 automatically — no legacy key needed
    const message = {
      token: fcmToken,

      // Shown in phone notification tray
      notification: {
        title: `🐄 Heat Alert — ${cowName}`,
        body:  `${cowName} is in heat! `
             + `Temp: ${temperature}°C · `
             + `Activity: ${activity} · `
             + `Score: ${heatScore}%\n`
             + `Act now — window: 6–18 hours`,
      },

      // Readable in app code when notification is tapped
      data: {
        type:        'HEAT_ALERT',
        cowId:       String(cowId),
        cowName:     String(cowName),
        temp:        String(temperature),
        activity:    String(activity),
        score:       String(heatScore),
        farmerId:    String(farmerId),
      },

      // Android: wake screen even in battery saver mode
      android: {
        priority: 'high',
        notification: {
          channelId:   'heat_alerts',
          priority:    'max',
          sound:       'default',
          color:       '#E05252',
          visibility:  'public',
        },
      },

      // iOS config
      apns: {
        payload: {
          aps: {
            sound:            'default',
            badge:            1,
            contentAvailable: true,
          },
        },
      },
    };

    const response = await admin.messaging().send(message);
    console.log(`[Alert] ✅ Notification sent to ${cowName}: ${response}`);
    res.json({ ok: true, sent: true, messageId: response });

  } catch (e) {
    console.error('[Alert] Failed:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ════════════════════════════════════════════════════════
//  GET /health
//  Simple check — Render pings this to keep server alive
// ════════════════════════════════════════════════════════
app.get('/health', (_, res) => {
  res.json({
    status:    'ok',
    service:   'CowWatch Backend',
    timestamp: new Date().toISOString(),
  });
});

// ── Start server ─────────────────────────────────────────
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`╔══════════════════════════════════════╗`);
  console.log(`║   CowWatch Server — Port ${PORT}        ║`);
  console.log(`║   FCM v1 API (non-deprecated)        ║`);
  console.log(`╚══════════════════════════════════════╝`);
});

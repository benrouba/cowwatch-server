"""
OptiFarm AI Server — XGBoost Heat Detection
Deploy to Railway.app (or any Python host)

Setup:
  1. Set environment variables:
       FIREBASE_SERVICE_ACCOUNT = { ...json... }
       FIREBASE_DATABASE_URL    = https://your-project.firebaseio.com
  2. pip install -r requirements.txt
  3. python ai_server.py

requirements.txt:
  flask
  xgboost
  scikit-learn
  joblib
  numpy
  firebase-admin
  requests
  gunicorn
"""

from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, db as rtdb, messaging
import numpy as np, joblib, os, json, datetime
from collections import deque
import threading, requests as req_lib

# ── Keep server warm (prevents cold starts) ───────────────────
def _keep_alive():
    import time
    while True:
        time.sleep(14 * 60)
        try:
            url = os.environ.get('RAILWAY_STATIC_URL',
                  os.environ.get('RENDER_EXTERNAL_URL', 'http://localhost:5000'))
            req_lib.get(f"{url}/health", timeout=10)
        except:
            pass

threading.Thread(target=_keep_alive, daemon=True).start()

# ── Model paths ───────────────────────────────────────────────
_DIR = os.path.dirname(os.path.abspath(__file__))
_MDL = os.path.join(_DIR, 'model_output')

# ── Firebase init ─────────────────────────────────────────────
cred = credentials.Certificate(
    json.loads(os.environ['FIREBASE_SERVICE_ACCOUNT']))
firebase_admin.initialize_app(cred, {
    'databaseURL': os.environ['FIREBASE_DATABASE_URL']})

# ── Load XGBoost model ────────────────────────────────────────
model     = joblib.load(os.path.join(_MDL, 'heat_model.pkl'))
scaler    = joblib.load(os.path.join(_MDL, 'scaler.pkl'))
threshold = joblib.load(os.path.join(_MDL, 'threshold.pkl'))

with open(os.path.join(_MDL, 'model_config.json'), encoding='utf-8') as f:
    cfg = json.load(f)

WINDOW   = cfg['window_samples']   # 300 (10 min @ 2s interval)
FEATURES = cfg['feature_cols']     # 16 features
G        = cfg.get('g_ms2', 9.81)
ACT_THR  = cfg.get('activity_thresh_ms2', 0.294)

# Per-cow sliding window buffer
buffers = {}

def get_buffer(cow_id):
    if cow_id not in buffers:
        buffers[cow_id] = deque(maxlen=WINDOW)
    return buffers[cow_id]

def valid_temp(t):
    return t is not None and 10.0 <= float(t) <= 42.0

def predict_heat(b):
    """
    Flatten the sliding window buffer into 80 statistical features
    and run XGBoost prediction. Returns probability 0.0–1.0.
    """
    seq = list(b)

    # Pad with first entry if buffer not full yet
    if len(seq) < WINDOW:
        seq = [seq[0]] * (WINDOW - len(seq)) + seq

    # Build array: (WINDOW, n_features)
    arr = np.array(
        [[s[f] for f in FEATURES] for s in seq],
        dtype=np.float32
    )

    # Compute 5 statistics per feature → 16 × 5 = 80 flat values
    row = []
    for fi in range(arr.shape[1]):
        col = arr[:, fi]
        row.extend([
            float(np.mean(col)),     # mean
            float(np.std(col)),      # std
            float(np.max(col)),      # max
            float(np.min(col)),      # min
            float(col[-1] - col[0]), # trend (end - start)
        ])

    # Scale and predict
    X_flat  = scaler.transform(np.array([row], dtype=np.float32))
    ai_prob = float(model.predict_proba(X_flat)[0][1])
    return ai_prob

# ── Flask app ─────────────────────────────────────────────────
app = Flask(__name__)

# ── /sensor — called by ESP32 every 5 seconds ─────────────────
@app.route('/sensor', methods=['POST'])
def sensor():
    try:
        d = request.get_json(force=True)
        if d is None:
            return jsonify({'status': 'error', 'reason': 'invalid JSON'}), 400

        cow_id = d.get('cowId',   'COW_01')
        temp   = float(d.get('temperature', 0))
        act    = int(d.get('activity', 0))
        ts     = int(d.get('timestamp', 0))

        # Reject invalid temperature (sensor disconnected = 55°C etc.)
        if not valid_temp(temp):
            return jsonify({'status': 'rejected', 'reason': 'invalid_temp'})

        # Time of day
        hour = datetime.datetime.fromtimestamp(ts / 1000).hour if ts > 0 \
               else datetime.datetime.now().hour

        # Update sliding window
        b     = get_buffer(cow_id)
        temps = [r['temperature'] for r in b] + [temp]
        acts  = [r['activity']    for r in b] + [act]
        s1h   = min(len(temps), 1800 // 2)
        s30m  = min(len(acts),   900 // 2)

        entry = {
            'temperature':   temp,
            'temp_rise_1h':  temp - float(np.mean(temps[-s1h:])),
            'ax_mean':  float(d.get('ax_mean',  0.0)),
            'ay_mean':  float(d.get('ay_mean',  0.0)),
            'az_mean':  float(d.get('az_mean',  G)),
            'ax_std':   float(d.get('ax_std',   0.1)),
            'ay_std':   float(d.get('ay_std',   0.1)),
            'az_std':   float(d.get('az_std',   0.1)),
            'mag_mean': float(d.get('mag_mean', G)),
            'mag_std':  float(d.get('mag_std',  0.1)),
            'mag_peak': float(d.get('mag_peak', G)),
            'activity':      act,
            'act_mean_30m':  float(np.mean(acts[-s30m:])),
            'act_trend_30m': float(act - np.mean(acts[-s30m:])),
            'hour_sin': float(np.sin(2 * np.pi * hour / 24)),
            'hour_cos': float(np.cos(2 * np.pi * hour / 24)),
        }
        b.append(entry)
        fill = len(b) / WINDOW

        # Run AI prediction if enough history (at least 25% of window)
        if len(b) >= WINDOW // 4:
            ai_prob = predict_heat(b)
        else:
            # Not enough history yet — fall back to rule-based score
            ai_prob = float(d.get('heatScore', 0)) / 100.0

        # Determine status from threshold
        if ai_prob >= threshold:
            status = 'HEAT'
        elif ai_prob >= threshold * 0.6:
            status = 'WATCH'
        else:
            status = 'NORMAL'

        score = round(ai_prob * 100, 2)

        print(f"[{cow_id}] temp={temp:.1f}°C act={act} "
              f"ai={score:.1f}% status={status} fill={fill:.0%}")

        # Write to Firebase /cows/{cowId}/latest
        payload = {
            'cowId':       cow_id,
            'cowName':     d.get('cowName', cow_id),
            'temperature': temp,
            'activity':    act,
            'heatScore':   score,
            'ruleScore':   d.get('heatScore', 0),
            'status':      status,
            'hwStatus':    d.get('hwStatus', 'OK'),
            'bmiReady':    d.get('bmiReady', False),
            'battery':     d.get('battery', 'UNKNOWN'),
            'aiConfidence': round(fill, 2),
            'timestamp':   ts,
        }
        rtdb.reference(f'/cows/{cow_id}/latest').set(payload)
        rtdb.reference(f'/cows/{cow_id}/history').push(payload)

        # Prune history to last 100 entries
        hist_ref = rtdb.reference(f'/cows/{cow_id}/history')
        hist     = hist_ref.get() or {}
        if len(hist) > 100:
            for k in sorted(hist.keys())[:len(hist) - 100]:
                hist_ref.child(k).delete()

        # FCM heat alert with 5-minute cooldown
        if status == 'HEAT':
            alert_ref  = rtdb.reference(f'/alerts/{cow_id}')
            last_alert = alert_ref.get() or {}
            cooldown   = 5 * 60 * 1000  # 5 minutes in ms
            if not last_alert or (ts - last_alert.get('timestamp', 0)) > cooldown:
                alert_ref.set({
                    'cowId':      cow_id,
                    'cowName':    d.get('cowName', cow_id),
                    'temperature': temp,
                    'activity':   act,
                    'heatScore':  score,
                    'notified':   True,
                    'timestamp':  ts,
                })
                try:
                    token = rtdb.reference('/farm/fcmToken').get()
                    if token:
                        messaging.send(messaging.Message(
                            notification=messaging.Notification(
                                title=f"🔴 Chaleur — {d.get('cowName', cow_id)}",
                                body=f"Temp: {temp:.1f}°C · Score IA: {score:.0f}%"
                                     f" · Inséminer dans 6–18h"),
                            data={
                                'type':    'HEAT_ALERT',
                                'cowId':   cow_id,
                                'cowName': d.get('cowName', cow_id),
                                'temp':    str(temp),
                                'score':   str(score),
                            },
                            token=token,
                        ))
                        print(f"[FCM] Alert sent for {cow_id}")
                except Exception as e:
                    print(f"[FCM] Error: {e}")

        return jsonify({
            'status':     'ok',
            'ai_score':   score,
            'ai_status':  status,
            'confidence': round(fill, 2),
        })

    except Exception as e:
        print(f"[/sensor ERROR] {e}")
        import traceback; traceback.print_exc()
        return jsonify({'status': 'error', 'reason': str(e)}), 500


# ── /alert — called by ESP32 for urgent alerts ────────────────
@app.route('/alert', methods=['POST'])
def alert():
    try:
        d = request.get_json(force=True)
        if d is None:
            return jsonify({'status': 'error', 'reason': 'invalid JSON'}), 400

        cow_id = d.get('cowId', 'COW_01')
        temp   = float(d.get('temperature', 0))
        score  = float(d.get('heatScore', 0))
        ts     = int(d.get('timestamp', 0))

        if not valid_temp(temp):
            return jsonify({'status': 'rejected', 'reason': 'invalid_temp'})

        rtdb.reference(f'/alerts/{cow_id}').set({
            'cowId':       cow_id,
            'cowName':     d.get('cowName', cow_id),
            'temperature': temp,
            'activity':    d.get('activity', 0),
            'heatScore':   score,
            'notified':    True,
            'timestamp':   ts,
        })

        try:
            token = rtdb.reference('/farm/fcmToken').get()
            if token:
                messaging.send(messaging.Message(
                    notification=messaging.Notification(
                        title=f"🔴 Alerte chaleur — {d.get('cowName', cow_id)}",
                        body=f"Temp: {temp:.1f}°C · Score: {score:.0f}%"),
                    data={'type': 'HEAT_ALERT', 'cowId': cow_id},
                    token=token,
                ))
        except Exception as e:
            print(f"[FCM] Alert error: {e}")

        return jsonify({'status': 'ok'})

    except Exception as e:
        print(f"[/alert ERROR] {e}")
        return jsonify({'status': 'error', 'reason': str(e)}), 500


# ── /health — status check ────────────────────────────────────
@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status':     'ok',
        'model':      'xgboost',
        'window_min': WINDOW * 2 // 60,
        'threshold':  round(float(threshold), 2),
        'features':   len(FEATURES),
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"OptiFarm AI Server starting on port {port}")
    print(f"Model window: {WINDOW} samples ({WINDOW*2//60} min)")
    print(f"Decision threshold: {threshold:.2f}")
    app.run(host='0.0.0.0', port=port)
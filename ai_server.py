"""ai_server.py — OptiFarm AI Server — deploy to Render.com
   pip install flask tensorflow scikit-learn joblib numpy firebase-admin
   Start: python ai_server.py
"""
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, db as rtdb, messaging
import numpy as np, joblib, os, json, datetime
from collections import deque
import threading, requests

# ── Keep-alive: ping self every 14 min to prevent Render sleep ──
def _keep_alive():
    import time
    while True:
        time.sleep(14 * 60)
        try:
            url = os.environ.get('RENDER_EXTERNAL_URL', 'http://localhost:5000')
            requests.get(f"{url}/health", timeout=10)
        except:
            pass

threading.Thread(target=_keep_alive, daemon=True).start()

# ── Model paths (relative to this file) ──
_DIR      = os.path.dirname(os.path.abspath(__file__))
_MDL      = os.path.join(_DIR, "model_output")

# ── Init Firebase ──────────────────────────────────────────────
cred = credentials.Certificate(
    json.loads(os.environ['FIREBASE_SERVICE_ACCOUNT']))
firebase_admin.initialize_app(cred, {
    'databaseURL': os.environ['FIREBASE_DATABASE_URL']})

# ── Load model ─────────────────────────────────────────────────
# Paths relative to project root (model_output/ is a subfolder)
model     = joblib.load(os.path.join(_MDL, 'heat_model.pkl'))
scaler    = joblib.load(os.path.join(_MDL, 'scaler.pkl'))
threshold = joblib.load(os.path.join(_MDL, 'threshold.pkl'))
with open(os.path.join(_MDL, 'model_config.json'), encoding='utf-8') as f:
    cfg = json.load(f)

WINDOW   = cfg['window_samples']
FEATURES = cfg['feature_cols']
G        = cfg.get('g_ms2', 9.81)
ACT_THR  = cfg.get('activity_thresh_ms2', 0.294)
buffers  = {}   # per-cow sliding window

def buf(cow_id):
    if cow_id not in buffers:
        buffers[cow_id] = deque(maxlen=WINDOW)
    return buffers[cow_id]

def valid_temp(t):
    return t is not None and 10.0 <= float(t) <= 42.0

app = Flask(__name__)

@app.route('/sensor', methods=['POST'])
def sensor():
    d      = request.get_json()
    cow_id = d.get('cowId', 'COW_01')
    temp   = float(d.get('temperature', 0))
    act    = int(d.get('activity', 0))
    ts     = int(d.get('timestamp', 0))

    if not valid_temp(temp):
        return jsonify({'status':'rejected','reason':'invalid_temp'})

    hour  = datetime.datetime.fromtimestamp(ts/1000).hour
    b     = buf(cow_id)
    temps = [r['temperature'] for r in b] + [temp]
    acts  = [r['activity']    for r in b] + [act]
    s1h   = min(len(temps), 1800//2)
    s30m  = min(len(acts),   900//2)

    entry = {
        'temperature':   temp,
        'temp_rise_1h':  temp - float(np.mean(temps[-s1h:])),
        'ax_mean':  float(d.get('ax_mean', 0)),
        'ay_mean':  float(d.get('ay_mean', 0)),
        'az_mean':  float(d.get('az_mean', G)),
        'ax_std':   float(d.get('ax_std',  0.1)),
        'ay_std':   float(d.get('ay_std',  0.1)),
        'az_std':   float(d.get('az_std',  0.1)),
        'mag_mean': float(d.get('mag_mean', G)),
        'mag_std':  float(d.get('mag_std',  0.1)),
        'mag_peak': float(d.get('mag_peak', G)),
        'activity':      act,
        'act_mean_30m':  float(np.mean(acts[-s30m:])),
        'act_trend_30m': float(act - np.mean(acts[-s30m:])),
        'hour_sin': float(np.sin(2*np.pi*hour/24)),
        'hour_cos': float(np.cos(2*np.pi*hour/24)),
    }
    b.append(entry)
    fill = len(b) / WINDOW

    if len(b) >= WINDOW // 4:
        seq = list(b)
        if len(seq) < WINDOW:
            seq = [seq[0]]*(WINDOW-len(seq)) + seq
        X  = np.array([[s[f] for f in FEATURES] for s in seq],
                      dtype=np.float32)
        X  = scaler.transform(X).reshape(1, WINDOW, len(FEATURES))
        ai_prob = float(model.predict(X, verbose=0)[0][0])
    else:
        ai_prob = float(d.get('heatScore',0)) / 100.0

    status = ('HEAT'  if ai_prob >= threshold else
              'WATCH' if ai_prob >= threshold*0.6 else 'NORMAL')
    score  = round(ai_prob*100, 2)

    payload = {**d,
        'heatScore': score, 'ruleScore': d.get('heatScore',0),
        'status': status, 'aiConfidence': round(fill,2),
        'bmiReady': d.get('bmiReady', False)}
    rtdb.reference(f'/cows/{cow_id}/latest').set(payload)
    rtdb.reference(f'/cows/{cow_id}/history').push(payload)

    # Prune history to 100
    hr = rtdb.reference(f'/cows/{cow_id}/history')
    h  = hr.get() or {}
    if len(h) > 100:
        for k in sorted(h.keys())[:len(h)-100]:
            hr.child(k).delete()

    # FCM alert with 5-min cooldown
    if status == 'HEAT':
        ar  = rtdb.reference(f'/alerts/{cow_id}')
        old = ar.get() or {}
        if not old or (ts - old.get('timestamp',0)) > 300000:
            ar.set({**d,'heatScore':score,'notified':True,'timestamp':ts})
            try:
                tok = rtdb.reference('/farm/fcmToken').get()
                if tok:
                    messaging.send(messaging.Message(
                        notification=messaging.Notification(
                            title=f"Chaleur — {d.get('cowName',cow_id)}",
                            body=f"Temp:{temp:.1f}°C · Score IA:{score:.0f}%"),
                        data={'type':'HEAT_ALERT','cowId':cow_id,
                              'score':str(score)},
                        token=tok))
            except Exception as e:
                print(f"FCM: {e}")

    return jsonify({'status':'ok','ai_score':score,'ai_status':status,
                    'confidence':round(fill,2)})

@app.route('/health')
def health():
    return jsonify({'status':'ok','model':'xgboost',
                    'window_min':WINDOW*2//60})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',5000)))
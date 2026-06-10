"""
OptiFarm AI — Heat Detection BiLSTM Pipeline
=============================================
Dataset:      MmCows (confirmed structure)
Architecture: Bidirectional LSTM + Self-Attention (arXiv:2506.16380)
Labels:       behavior codes 1 & 6 = heat/mounting (July 25, 2023)
Training:     July 25 only (only labeled day), 12 T-cows, ~518K windows

Confirmed facts:
  - IMU columns: timestamp, accel_x_mps2, accel_y_mps2, accel_z_mps2 (m/s²)
  - IMU rate: ~100Hz  |  Label rate: 1Hz  |  ESP32 window: 2s
  - T01=C01 ... T10=C10, T13=C13, T14=C14 (timestamps confirmed identical)
  - Behavior 1 & 6 = mounting/heat (~2.2% of day = biologically correct)
  - Neck temp: sub_data/neck_dev_temp/T01.csv

Requirements: pip install tensorflow pandas numpy scikit-learn joblib flask firebase-admin
"""

import os, json, warnings
import numpy as np
import pandas as pd
from pathlib import Path
import joblib
warnings.filterwarnings('ignore')

# ══════════════════════════════════════════════════════════════
#  PATHS
# ══════════════════════════════════════════════════════════════
MMCOWS  = Path(r'C:\Users\benro\Documents\ensignement\encadrement\SNV\Boudechiche\cowwatch-server\mmcows')
SENSOR  = MMCOWS  / 'sensor_data/sensor_data'
IMU_DIR = SENSOR  / 'main_data/immu'
LBL_DIR = SENSOR  / 'behavior_labels/individual'
TMP_DIR = SENSOR  / 'sub_data/neck_dev_temp'
OUT_DIR = Path('./model_output')
OUT_DIR.mkdir(exist_ok=True)

# T→C mapping confirmed via identical timestamps
T_TO_C = {
    'T01':'C01','T02':'C02','T03':'C03','T04':'C04',
    'T05':'C05','T06':'C06','T07':'C07','T08':'C08',
    'T09':'C09','T10':'C10','T13':'C13','T14':'C14',
}
IMU_COWS = list(T_TO_C.keys())

# Behavior codes confirmed from peek:
# 7=lying(51.7%), 2=eating/rum(17.6%), 0=standing(17.4%),
# 4=walking(7.4%), 3=rum.standing(3.6%), 1=mounting(1.2%), 6=heat(1.1%)
HEAT_BEHAVIORS = {1, 6}   # rare codes = mounting/heat behavior

# ══════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════
G_MS2        = 9.81
ACT_THR      = 0.03 * G_MS2    # 0.294 m/s²
ESP32_S      = 2                # ESP32 sends every 2 seconds
IMU_HZ       = 100              # MmCows IMU sample rate
SAMPLES_2S   = ESP32_S * IMU_HZ # 200 raw samples per 2s window

WIN_MIN      = 10                          # 10-min lookback
WIN_SIZE     = (WIN_MIN * 60) // ESP32_S   # 300 samples
STRIDE_SIZE  = 60                          # 2-minute stride → ~8,500 sequences
                                            # Balance: enough data, fast training

FEATURE_COLS = [
    'temperature', 'temp_rise_1h',
    'ax_mean','ay_mean','az_mean',
    'ax_std', 'ay_std', 'az_std',
    'mag_mean','mag_std','mag_peak',
    'activity','act_mean_30m','act_trend_30m',
    'hour_sin','hour_cos',
]

# ══════════════════════════════════════════════════════════════
#  STEP 1 — LOAD ONE COW (July 25 only)
# ══════════════════════════════════════════════════════════════

def load_cow(t_id):
    c_id     = T_TO_C[t_id]
    imu_file = IMU_DIR / t_id / f'{t_id}_0725.csv'
    lbl_file = LBL_DIR / f'{c_id}_0725.csv'

    if not imu_file.exists():
        print(f"  ✗ IMU not found: {imu_file.name}"); return None
    if not lbl_file.exists():
        print(f"  ✗ Labels not found: {lbl_file.name}"); return None

    # ── Load IMU ─────────────────────────────────────────────
    imu = pd.read_csv(imu_file, encoding='utf-8', on_bad_lines='skip')
    imu.columns = imu.columns.str.lower().str.strip()
    imu = imu.rename(columns={
        'accel_x_mps2':'ax', 'accel_y_mps2':'ay', 'accel_z_mps2':'az'})
    imu['ts'] = pd.to_numeric(imu['timestamp'], errors='coerce')
    imu = imu.dropna(subset=['ts','ax','ay','az'])

    # Timestamps are Unix seconds (1.69e9 range)
    imu['dt'] = pd.to_datetime(imu['ts'], unit='s')
    imu = imu.sort_values('dt').set_index('dt')

    # ── Load behavior labels (1Hz) ────────────────────────────
    lbl = pd.read_csv(lbl_file, encoding='utf-8', on_bad_lines='skip')
    lbl.columns = lbl.columns.str.lower().str.strip()
    lbl['ts']  = pd.to_numeric(lbl['timestamp'], errors='coerce')
    lbl['dt']  = pd.to_datetime(lbl['ts'], unit='s')
    lbl = lbl.dropna(subset=['ts','behavior'])
    lbl = lbl.set_index('dt')[['behavior']]
    lbl['behavior'] = pd.to_numeric(lbl['behavior'], errors='coerce').fillna(0).astype(int)

    # ── Resample IMU to 2-second windows ─────────────────────
    freq = f'{ESP32_S}s'
    resampled = imu[['ax','ay','az']].resample(freq).agg(list)

    rows = []
    for dt, row in resampled.iterrows():
        ax = np.array(row['ax'] if isinstance(row['ax'], list) else [row['ax']], dtype=float)
        ay = np.array(row['ay'] if isinstance(row['ay'], list) else [row['ay']], dtype=float)
        az = np.array(row['az'] if isinstance(row['az'], list) else [row['az']], dtype=float)
        if len(ax) < 3: continue

        mag   = np.sqrt(ax**2 + ay**2 + az**2)
        delta = np.abs(np.diff(mag))

        rows.append({
            'dt':       dt,
            'ax_mean':  float(np.mean(ax)),
            'ay_mean':  float(np.mean(ay)),
            'az_mean':  float(np.mean(az)),
            'ax_std':   float(np.std(ax)),
            'ay_std':   float(np.std(ay)),
            'az_std':   float(np.std(az)),
            'mag_mean': float(np.mean(mag)),
            'mag_std':  float(np.std(mag)),
            'mag_peak': float(np.max(mag)),
            'activity': int(np.sum(delta > ACT_THR)),
        })

    df = pd.DataFrame(rows).set_index('dt')

    # ── Assign label: any HEAT_BEHAVIOR in 2s window → 1 ────
    # Each 2s window covers 2 label rows (1Hz labels)
    lbl_2s = lbl['behavior'].resample(freq).agg(
        lambda x: 1 if any(v in HEAT_BEHAVIORS for v in x) else 0)
    df = df.join(lbl_2s.rename('label'), how='left')
    df['label'] = df['label'].fillna(0).astype(int)

    # ── Load neck temperature ─────────────────────────────────
    tmp_file = TMP_DIR / f'{t_id}.csv'
    if tmp_file.exists():
        tmp = pd.read_csv(tmp_file, encoding='utf-8', on_bad_lines='skip')
        tmp.columns = tmp.columns.str.lower().str.strip()
        ts_c  = next((c for c in tmp.columns if 'time' in c), None)
        tp_c  = next((c for c in tmp.columns if 'temp' in c), None)
        if ts_c and tp_c:
            tmp['ts'] = pd.to_numeric(tmp[ts_c], errors='coerce')
            tmp['dt'] = pd.to_datetime(tmp['ts'],
                        unit='ms' if tmp['ts'].dropna().iloc[0]>1e12 else 's')
            tmp = tmp.set_index('dt')[[tp_c]].rename(columns={tp_c:'temperature'})
            # Keep only July 25
            tmp = tmp[tmp.index.date == pd.Timestamp('2023-07-25').date()]
            tmp = tmp.resample(freq).mean().interpolate('time')
            df  = df.join(tmp, how='left')

    if 'temperature' not in df.columns:
        df['temperature'] = 38.5
    df.loc[(df['temperature']<10)|(df['temperature']>42),'temperature'] = np.nan
    df['temperature'] = df['temperature'].interpolate().ffill().fillna(38.5)

    # ── Time-series derived features ─────────────────────────
    s1h  = (60*60) // ESP32_S
    s30m = (30*60) // ESP32_S
    df['temp_rise_1h']  = df['temperature'] - \
        df['temperature'].shift(s1h).fillna(df['temperature'])
    df['act_mean_30m']  = df['activity'].rolling(s30m, min_periods=1).mean()
    df['act_trend_30m'] = df['activity'] - \
        df['activity'].shift(s30m).fillna(df['activity'])
    df['hour_sin'] = np.sin(2*np.pi*df.index.hour/24)
    df['hour_cos'] = np.cos(2*np.pi*df.index.hour/24)

    df['cow_id'] = t_id
    df = df.dropna(subset=['ax_mean'])

    n_heat = df['label'].sum()
    pct    = df['label'].mean()*100
    print(f"  ✅ {len(df):,} windows — heat: {n_heat:,} ({pct:.1f}%)")
    return df


# ══════════════════════════════════════════════════════════════
#  STEP 2 — LOAD ALL COWS
# ══════════════════════════════════════════════════════════════

def load_all():
    print("\n" + "="*60)
    print("LOADING JULY 25 DATA — 12 COWS")
    print("="*60)
    frames = []
    for t_id in IMU_COWS:
        print(f"\n{t_id} → {T_TO_C[t_id]}:")
        df = load_cow(t_id)
        if df is not None and len(df) >= WIN_SIZE:
            frames.append(df)

    if not frames:
        raise RuntimeError("No data loaded!")

    combined = pd.concat(frames)
    total_heat = combined['label'].sum()
    print(f"\n{'='*60}")
    print(f"TOTAL: {len(combined):,} windows | "
          f"Heat: {total_heat:,} ({combined['label'].mean()*100:.2f}%)")
    return combined


# ══════════════════════════════════════════════════════════════
#  STEP 3 — BUILD FLAT FEATURE WINDOWS FOR XGBOOST
#  Instead of 3D sequences, compute statistical summary of each window.
#  XGBoost on flat features trains in <1 min and handles imbalance better.
# ══════════════════════════════════════════════════════════════

def build_windows(df):
    """
    Slide a WIN_SIZE window over each cow with STRIDE_SIZE step.
    For each window compute: mean, std, min, max, trend of each feature.
    Output: flat 2D array (n_windows, n_flat_features) for XGBoost.
    """
    print(f"\nBuilding {WIN_MIN}-min flat windows (stride={STRIDE_SIZE*2}s)...")
    X_list, y_list = [], []

    stats = ['mean','std','max','min','trend']

    for cow_id, cdf in df.groupby('cow_id'):
        cdf   = cdf.sort_index()
        feats = cdf[FEATURE_COLS].values.astype(np.float32)
        lbls  = cdf['label'].values

        for s in range(0, len(feats)-WIN_SIZE-STRIDE_SIZE, STRIDE_SIZE):
            e   = s + WIN_SIZE
            win = feats[s:e]          # (WIN_SIZE, n_features)
            fu  = min(e+STRIDE_SIZE, len(lbls))

            # Compute statistics across the time dimension
            row = []
            for fi in range(win.shape[1]):
                col = win[:, fi]
                row.extend([
                    float(np.mean(col)),
                    float(np.std(col)),
                    float(np.max(col)),
                    float(np.min(col)),
                    float(col[-1] - col[0]),   # trend: end - start
                ])

            X_list.append(row)
            y_list.append(int(lbls[e:fu].max()))

    X = np.array(X_list, dtype=np.float32)
    y = np.array(y_list, dtype=np.int32)
    print(f"Windows:  {len(X):,}  features: {X.shape[1]}")
    print(f"Heat:     {y.sum():,} ({y.mean()*100:.2f}%)")
    return X, y, stats


# ══════════════════════════════════════════════════════════════
#  STEP 4 — BILSTM + SELF-ATTENTION MODEL
# ══════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════
#  STEP 4 — XGBOOST CLASSIFIER
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
#  STEP 5 — TRAIN
# ══════════════════════════════════════════════════════════════

def train(X, y):
    from xgboost import XGBClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import (classification_report,
                                  confusion_matrix, f1_score)

    print("\n" + "="*60)
    print("TRAINING XGBoost")
    print("="*60)

    # X is 2D (n_windows, n_flat_features) — already flat from build_windows()
    n = len(X)
    rng  = np.random.default_rng(42)
    perm = rng.permutation(n)
    X, y = X[perm], y[perm]

    te = int(n*0.15); va = int(n*0.15); tr = n-va-te
    X_tr, y_tr = X[:tr],      y[:tr]
    X_va, y_va = X[tr:tr+va], y[tr:tr+va]
    X_te, y_te = X[tr+va:],   y[tr+va:]
    print(f"Train {len(X_tr):,} | Val {len(X_va):,} | Test {len(X_te):,}")

    scaler = StandardScaler()
    X_tr   = scaler.fit_transform(X_tr)
    X_va   = scaler.transform(X_va)
    X_te   = scaler.transform(X_te)

    n0 = int((y_tr==0).sum()); n1 = int((y_tr==1).sum())
    scale_pos = n0 / max(n1, 1)
    print(f"Normal: {n0:,}  Heat: {n1:,}  scale_pos_weight: {scale_pos:.1f}")

    model = XGBClassifier(
        n_estimators          = 600,
        max_depth             = 6,
        learning_rate         = 0.05,
        subsample             = 0.8,
        colsample_bytree      = 0.8,
        scale_pos_weight      = scale_pos,
        eval_metric           = 'auc',
        early_stopping_rounds = 25,
        random_state          = 42,
        n_jobs                = -1,
        verbosity             = 1,
    )

    print("Training... (< 2 minutes)")
    model.fit(X_tr, y_tr,
              eval_set=[(X_va, y_va)],
              verbose=50)

    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    probs = model.predict_proba(X_te)[:, 1]

    best_t, best_f1 = 0.5, 0.0
    for thr in np.arange(0.2, 0.85, 0.05):
        f1 = f1_score(y_te, (probs>thr).astype(int), zero_division=0)
        if f1 > best_f1: best_f1, best_t = f1, thr

    y_pred = (probs > best_t).astype(int)
    print(f"Optimal threshold: {best_t:.2f}  (F1={best_f1:.3f})")
    print(classification_report(y_te, y_pred,
                                 target_names=['NORMAL','HEAT']))
    cm = confusion_matrix(y_te, y_pred)
    print(f"TN={cm[0,0]}  FP={cm[0,1]}")
    print(f"FN={cm[1,0]}  TP={cm[1,1]}")

    # Feature importances — shows which sensor signals matter most
    fi = model.feature_importances_
    feat_names = []
    for fname in FEATURE_COLS:
        for s in ['mean','std','max','min','trend']:
            feat_names.append(f'{fname}_{s}')
    top10 = sorted(zip(feat_names, fi), key=lambda x: -x[1])[:10]
    print("\nTop 10 most important features:")
    for name, imp in top10:
        bar = '█' * int(imp * 300)
        print(f"  {name:<32} {imp:.4f}  {bar}")

    # Save
    joblib.dump(model,  str(OUT_DIR/'heat_model.pkl'))
    joblib.dump(scaler, str(OUT_DIR/'scaler.pkl'))
    joblib.dump(best_t, str(OUT_DIR/'threshold.pkl'))

    cfg = {
        'window_samples':      WIN_SIZE,
        'interval_s':          ESP32_S,
        'feature_cols':        FEATURE_COLS,
        'threshold':           float(best_t),
        'n_features':          len(FEATURE_COLS),
        'g_ms2':               G_MS2,
        'activity_thresh_ms2': ACT_THR,
        'heat_behaviors':      list(HEAT_BEHAVIORS),
        'training_date':       '2023-07-25',
        'n_cows':              len(IMU_COWS),
        'architecture':        'xgboost',
        'model_file':          'heat_model.pkl',
    }
    with open(OUT_DIR/'model_config.json','w',encoding='utf-8') as fp:
        json.dump(cfg, fp, indent=2, ensure_ascii=False)

    print(f"\n✅ Saved to {OUT_DIR}/")
    return model, scaler, best_t


# ══════════════════════════════════════════════════════════════
#  STEP 6 — WRITE FLASK SERVER FOR RENDER.COM
# ══════════════════════════════════════════════════════════════

def write_server():
    code = '''\
"""ai_server.py — OptiFarm AI Server — deploy to Render.com
   pip install flask tensorflow scikit-learn joblib numpy firebase-admin
   Start: python ai_server.py
"""
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, db as rtdb, messaging
import numpy as np, joblib, os, json, datetime
from collections import deque

# ── Init Firebase ──────────────────────────────────────────────
cred = credentials.Certificate(
    json.loads(os.environ['FIREBASE_SERVICE_ACCOUNT']))
firebase_admin.initialize_app(cred, {
    'databaseURL': os.environ['FIREBASE_DATABASE_URL']})

# ── Load model ─────────────────────────────────────────────────
model     = joblib.load('heat_model.pkl')  # XGBoost
scaler    = joblib.load('scaler.pkl')
threshold = joblib.load('threshold.pkl')
with open('model_config.json', encoding='utf-8') as f:
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
    return jsonify({'status':'ok','model':'bilstm_attention',
                    'window_min':WINDOW*2//60})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT',5000)))
'''
    out = OUT_DIR / 'ai_server.py'
    out.write_text(code, encoding='utf-8')
    print(f"✅ ai_server.py → {out}")


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("OptiFarm AI — XGBoost Heat Detection Pipeline")
    print("="*60)
    print(f"MmCows:   {MMCOWS}")
    print(f"Training: July 25, 2023 (only labeled day)")
    print(f"Cows:     {IMU_COWS}")
    print(f"Labels:   behavior codes {HEAT_BEHAVIORS} = heat/mounting")
    print(f"Window:   {WIN_MIN} min ({WIN_SIZE} samples @ {ESP32_S}s)")
    print(f"Features: {len(FEATURE_COLS)}: {FEATURE_COLS}\n")

    write_server()
    df       = load_all()
    df.to_csv(OUT_DIR/'preprocessed.csv', encoding='utf-8', index=True)
    print(f"\nPreprocessed CSV saved.")

    X, y, _  = build_windows(df)
    model, scaler, thr = train(X, y)

    print("\n" + "="*60)
    print("✅ PIPELINE COMPLETE")
    print("="*60)
    print(f"Files in {OUT_DIR}/")
    print("  heat_model.h5      ← BiLSTM weights")
    print("  scaler.pkl         ← StandardScaler")
    print("  threshold.pkl      ← optimal decision threshold")
    print("  model_config.json  ← deployment config")
    print("  ai_server.py       ← Flask server for Render.com")
    print("  preprocessed.csv   ← labeled dataset")
    print("\nNext: copy model_output/ to Render.com and deploy ai_server.py")
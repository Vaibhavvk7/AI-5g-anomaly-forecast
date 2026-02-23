import json
import time
import yaml
import joblib
import pandas as pd
from collections import deque, defaultdict

from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server

from src.utils.schema import NUMERIC_FIELDS, validate_event
from src.features.feature_builder import build_features
from src.monitoring.prometheus_metrics import (
    EVENTS_CONSUMED, ANOMALIES_FOUND, INFER_LATENCY, ANOMALY_RATE_GAUGE
)

def main():
    kafka_cfg = yaml.safe_load(open("configs/kafka.yaml"))
    feat_cfg = yaml.safe_load(open("configs/features.yaml"))
    model_cfg = yaml.safe_load(open("configs/model.yaml"))

    # Prometheus endpoint
    start_http_server(8000)
    print("Prometheus metrics on http://localhost:8000/metrics")

    # Load trained models if present
    iso_bundle = None
    forecast_bundle = None
    try:
        iso_bundle = joblib.load("artifacts/isoforest.joblib")
        print("Loaded anomaly model artifacts/isoforest.joblib")
    except Exception:
        print("No anomaly model found yet. Train later with src/train/train_anomaly.py")

    try:
        forecast_bundle = joblib.load("artifacts/forecast_models.joblib")
        print("Loaded forecast models artifacts/forecast_models.joblib")
    except Exception:
        print("No forecast model found yet. Train later with src/train/train_forecast.py")

    consumer = Consumer({
        "bootstrap.servers": kafka_cfg["bootstrap_servers"],
        "group.id": kafka_cfg["consumer_group"],
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([kafka_cfg["topic_telemetry"]])

    producer = Producer({"bootstrap.servers": kafka_cfg["bootstrap_servers"]})

    # Rolling buffers per gnb_id
    window_len = 300  # last 5 minutes at 1 msg/sec
    buffers = defaultdict(lambda: deque(maxlen=window_len))

    # Capture to parquet for offline training
    capture = True
    capture_rows = []
    last_flush = time.time()

    recent_flags = deque(maxlen=500)

    print("Consuming telemetry from:", kafka_cfg["topic_telemetry"])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            t0 = time.time()
        
            # run anomaly + forecast inference
            INFER_LATENCY.observe(time.time() - t0)
            evt = json.loads(msg.value().decode("utf-8"))
            validate_event(evt)

            EVENTS_CONSUMED.inc()
            buffers[evt["gnb_id"]].append(evt)

            if capture:
                capture_rows.append(evt)

            # Need some history
            if len(buffers[evt["gnb_id"]]) < 60:
                continue

            df = pd.DataFrame(list(buffers[evt["gnb_id"]])).sort_values("ts")

            # Anomaly inference
            is_anom = 0
            score = None
            if iso_bundle is not None:
                X = df[NUMERIC_FIELDS].astype("float32").tail(1)
                score = float(iso_bundle["model"].decision_function(X)[0])
                is_anom = int(score < float(iso_bundle["threshold"]))

            # Forecast inference
            forecasts = {}
            if forecast_bundle is not None:
                feat_df = build_features(df, lags=feat_cfg["lags"], rolling_windows=feat_cfg["rolling_windows"]).dropna()
                if len(feat_df) > 0:
                    xrow = feat_df.tail(1).copy()
                    if "gnb_id" in xrow.columns:
                        xrow["gnb_id"] = xrow["gnb_id"].astype("category").cat.codes
                    xrow = xrow.drop(columns=["scenario"], errors="ignore")
                    xrow = xrow.drop(columns=["ts", "region"], errors="ignore")
                    xnum = xrow.select_dtypes(include=["number"]).astype("float32")

                    for h, bundle in forecast_bundle.items():
                        cols = bundle["feature_cols"]
                        xh = xnum.reindex(columns=cols, fill_value=0.0)
                        yhat = float(bundle["model"].predict(xh)[0])
                        forecasts[str(h)] = yhat

            # Publish anomaly event
            if is_anom == 1:
                ANOMALIES_FOUND.inc()
                out = {
                    "ts": evt["ts"],
                    "gnb_id": evt["gnb_id"],
                    "region": evt["region"],
                    "scenario": evt["scenario"],
                    "anomaly": 1,
                    "anomaly_score": score,
                    "snapshot": {k: evt[k] for k in NUMERIC_FIELDS}
                }
                producer.produce(kafka_cfg["topic_anomalies"], json.dumps(out).encode("utf-8"))

            # Publish forecasts
            if forecasts:
                out = {
                    "ts": evt["ts"],
                    "gnb_id": evt["gnb_id"],
                    "region": evt["region"],
                    "target": model_cfg["forecast"]["target"],
                    "horizon_preds": forecasts
                }
                producer.produce(kafka_cfg["topic_forecasts"], json.dumps(out).encode("utf-8"))

            producer.poll(0)
            producer.flush(0)

            # Metrics
            recent_flags.append(is_anom)
            ANOMALY_RATE_GAUGE.set(sum(recent_flags) / max(1, len(recent_flags)))
            INFER_LATENCY.observe(time.time() - t0)
            # Capture flush
            if capture and (time.time() - last_flush) > 20:
                pd.DataFrame(capture_rows).to_parquet("data/raw/telemetry.parquet", index=False)
                last_flush = time.time()

            _ = t0  # keep if you want to measure

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

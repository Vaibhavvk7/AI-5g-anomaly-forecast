import os
import yaml
import joblib
import pandas as pd
import mlflow
from sklearn.ensemble import IsolationForest
from src.utils.schema import NUMERIC_FIELDS

def main():
    model_cfg = yaml.safe_load(open("configs/model.yaml"))["anomaly"]
    mlflow.set_tracking_uri("http://localhost:5001")
    mlflow.set_experiment("5g_anomaly_detection")

    # Expect telemetry snapshots saved from consumer or offline capture
    path = "data/raw/telemetry.parquet"
    if not os.path.exists(path):
        raise FileNotFoundError(
            "Missing data/raw/telemetry.parquet. "
            "Run consumer in 'capture' mode (next step) or save telemetry first."
        )

    df = pd.read_parquet(path).sort_values(["gnb_id", "ts"])
    X = df[NUMERIC_FIELDS].astype("float32")

    with mlflow.start_run():
        model = IsolationForest(
            n_estimators=300,
            contamination=float(model_cfg["contamination"]),
            random_state=int(model_cfg["random_state"])
        )
        model.fit(X)

        # Score: higher => more normal; we convert to anomaly flag using percentile threshold
        scores = model.decision_function(X)
        thresh = float(pd.Series(scores).quantile(0.01))
        preds = (scores < thresh).astype(int)

        anomaly_rate = float(preds.mean())

        mlflow.log_params(model_cfg)
        mlflow.log_metric("anomaly_rate_est", anomaly_rate)
        mlflow.log_metric("score_threshold_q01", thresh)

        os.makedirs("artifacts", exist_ok=True)
        joblib.dump({"model": model, "threshold": thresh}, "artifacts/isoforest.joblib")
        try:
            mlflow.log_artifact("artifacts/isoforest.joblib")
        except Exception as e:
            print(f"[mlflow] log_artifact skipped: {e}")

        print("Saved artifacts/isoforest.joblib")
        print("Estimated anomaly rate:", anomaly_rate)
        mlflow.set_registered_model_alias("5g-anomaly-isoforest", "champion", f"models:/5g-anomaly-isoforest/{version}")

if __name__ == "__main__":
    main()

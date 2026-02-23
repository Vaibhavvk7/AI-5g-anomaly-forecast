import os
import yaml
import joblib
import pandas as pd
import mlflow
from lightgbm import LGBMRegressor
from src.features.feature_builder import build_features

def main():
    cfg = yaml.safe_load(open("configs/model.yaml"))["forecast"]
    fcfg = yaml.safe_load(open("configs/features.yaml"))
    horizons = cfg["horizons"]
    target = cfg["target"]

    mlflow.set_tracking_uri("http://localhost:5001")
    mlflow.set_experiment("5g_forecasting")

    path = "data/raw/telemetry.parquet"
    if not os.path.exists(path):
        raise FileNotFoundError("Missing data/raw/telemetry.parquet. Capture telemetry first.")

    df = pd.read_parquet(path).sort_values(["gnb_id", "ts"])
    # Build features on raw telemetry
    feat_df = build_features(df, lags=fcfg["lags"], rolling_windows=fcfg["rolling_windows"])

    # Base cleanup
    feat_df = feat_df.dropna().reset_index(drop=True)

    models = {}

    with mlflow.start_run():
        mlflow.log_params(cfg["lightgbm_params"])
        mlflow.log_param("target", target)
        mlflow.log_param("horizons", horizons)

        for h in horizons:
            y = feat_df.groupby("gnb_id")[target].shift(-h)
            X = feat_df.drop(columns=["scenario"])  # keep ids for now; we’ll remove non-numeric
            # drop target leakage columns not needed
            X = X.drop(columns=["ts", "region"], errors="ignore")

            # align
            keep = y.notna()
            Xh = X.loc[keep].copy()
            yh = y.loc[keep].astype("float32")

            # encode gnb_id simply (categorical id as category codes)
            if "gnb_id" in Xh.columns:
                Xh["gnb_id"] = Xh["gnb_id"].astype("category").cat.codes

            # remove non-numeric columns if any
            Xh = Xh.select_dtypes(include=["number"]).astype("float32")

            model = LGBMRegressor(
                n_estimators=500,
                learning_rate=0.05,
                num_leaves=31,
                min_data_in_leaf=20,
                min_gain_to_split=0.0,
                subsample=0.9,
                colsample_bytree=0.9,
                random_state=42,
            )
            model.fit(Xh, yh)

            pred = model.predict(Xh)
            mae = float((abs(pred - yh)).mean())

            mlflow.log_metric(f"mae_h{h}", mae)
            models[h] = {"model": model, "feature_cols": list(Xh.columns)}

            print(f"h={h} MAE={mae:.4f}")
        print("TRACKING:", mlflow.get_tracking_uri())
        print("ARTIFACT_URI:", mlflow.get_artifact_uri())

        os.makedirs("artifacts", exist_ok=True)
        joblib.dump(models, "artifacts/forecast_models.joblib")
        try:
            mlflow.log_artifact("artifacts/forecast_models.joblib")
        except Exception as e:
            print(f"[mlflow] log_artifact skipped: {e}")
        print("Saved artifacts/forecast_models.joblib")

if __name__ == "__main__":
    main()

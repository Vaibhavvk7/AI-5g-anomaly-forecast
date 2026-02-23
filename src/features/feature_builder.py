import pandas as pd

def build_features(
    df: pd.DataFrame,
    lags=(1, 3, 12),
    rolling_windows=(6, 12, 24),
):
    """
    Efficient feature builder: avoids DataFrame fragmentation by building all
    derived columns into a dict and concatenating once.

    Expected columns: ts, gnb_id, plus numeric telemetry columns.
    """
    out = df.copy()
    out = out.sort_values(["gnb_id", "ts"]).reset_index(drop=True)

    numeric_cols = out.select_dtypes(include=["number"]).columns.tolist()

    feats = {}

    # Lag features
    g = out.groupby("gnb_id", sort=False)
    for col in numeric_cols:
        s = g[col]
        for L in lags:
            feats[f"{col}_lag{L}"] = s.shift(L)

    # Rolling features (shift by 1 to avoid leakage)
    for col in numeric_cols:
        shifted = g[col].shift(1)
        # rolling needs grouping preserved; use groupby + rolling then drop extra index
        for w in rolling_windows:
            feats[f"{col}_rmean{w}"] = (
                shifted.groupby(out["gnb_id"]).rolling(w).mean().reset_index(level=0, drop=True)
            )
            feats[f"{col}_rstd{w}"] = (
                shifted.groupby(out["gnb_id"]).rolling(w).std().reset_index(level=0, drop=True)
            )

    feat_df = pd.DataFrame(feats, index=out.index)
    return pd.concat([out, feat_df], axis=1)

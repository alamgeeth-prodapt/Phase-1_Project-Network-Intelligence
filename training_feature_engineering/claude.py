"""
Next-hour traffic forecasting + anomaly detection on the Milano telecom grid dataset.

Target:      internet[cell, t+1]
Model:       LightGBM regression on log1p(internet)
Anomaly:     residual z-score using a strictly-past rolling baseline

Leak-safety rule enforced throughout:
    every feature for predicting (cell, t+1) uses ONLY data with timestamp <= t.
    total_sms / total_calls / total_activity are algebraic sums of the raw
    columns and are NEVER used at the target timestamp — only as lagged
    predictors (at t or earlier).
"""

import glob
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree
from sklearn.cluster import KMeans
import lightgbm as lgb

warnings.filterwarnings("ignore")

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

DATA_DIR = Path("D:\phase_1_project\data_cleaning\processed\grid_hour")
CSV_GLOB = str(DATA_DIR / "*grid_hour.csv")
GEOJSON_PATH = "D:\phase_1_project\data_set\grid\milano-grid.geojson"

TRAIN_DATES = {"2013-11-01", "2013-11-02", "2013-11-03", "2013-11-04", "2013-11-05"}
VAL_DATES = {"2013-11-06"}
TEST_DATES = {"2013-11-07"}

N_CLUSTERS = 8
N_NEIGHBORS = 8         # 8-connected grid neighborhood
ANOMALY_Z_THRESHOLD = 3.0
RESIDUAL_BASELINE_WINDOW = 24   # hours, strictly past, per cell

RANDOM_STATE = 42


# ----------------------------------------------------------------------------
# 1. Load and concatenate all days into one long panel
# ----------------------------------------------------------------------------

def load_all_days(csv_glob: str) -> pd.DataFrame:
    files = sorted(glob.glob(csv_glob))
    if not files:
        raise FileNotFoundError(f"No files matched {csv_glob}")

    frames = []
    for f in files:
        df = pd.read_csv(
            f,
            usecols=[
                "grid_id", "date", "hour", "day_of_week",
                "internet", "total_calls", "total_sms",
            ],
        )
        frames.append(df)

    full = pd.concat(frames, ignore_index=True)

    day_order = {d: i for i, d in enumerate(sorted(full["date"].unique()))}
    full["day_idx"] = full["date"].map(day_order)
    full["hour_index"] = full["day_idx"] * 24 + full["hour"]

    full = full.sort_values(["grid_id", "hour_index"]).reset_index(drop=True)
    return full


# ----------------------------------------------------------------------------
# 2. Spatial adjacency from the geojson grid geometry
# ----------------------------------------------------------------------------

def build_adjacency(geojson_path: Path, k: int = N_NEIGHBORS) -> dict:
    with open(geojson_path) as f:
        gj = json.load(f)

    cell_ids = []
    centroids = []
    for feat in gj["features"]:
        coords = feat["geometry"]["coordinates"][0]  # exterior ring
        xs = [c[0] for c in coords]
        ys = [c[1] for c in coords]
        centroids.append((sum(xs) / len(xs), sum(ys) / len(ys)))
        cell_ids.append(feat["properties"]["cellId"])

    centroids = np.array(centroids)
    cell_ids = np.array(cell_ids)

    tree = cKDTree(centroids)
    dists, _ = tree.query(centroids, k=2)
    grid_step = np.median(dists[:, 1])
    radius = grid_step * 1.5  

    adjacency = {}
    for i, cid in enumerate(cell_ids):
        idxs = tree.query_ball_point(centroids[i], r=radius)
        neighbor_ids = [int(cell_ids[j]) for j in idxs if cell_ids[j] != cid]
        adjacency[int(cid)] = neighbor_ids[:k]

    return adjacency


def add_neighbor_feature(df: pd.DataFrame, adjacency: dict) -> pd.DataFrame:
    pivot = df.pivot(index="hour_index", columns="grid_id", values="internet")

    neighbor_mean = pd.DataFrame(index=pivot.index, columns=pivot.columns, dtype=float)
    for cid in pivot.columns:
        neighbors = adjacency.get(int(cid), [])
        neighbors = [n for n in neighbors if n in pivot.columns]
        if neighbors:
            neighbor_mean[cid] = pivot[neighbors].mean(axis=1)
        else:
            neighbor_mean[cid] = np.nan

    stacked = neighbor_mean.stack().rename("neighbor_mean_internet_t").reset_index()
    stacked.columns = ["hour_index", "grid_id", "neighbor_mean_internet_t"]

    df = df.merge(stacked, on=["hour_index", "grid_id"], how="left")
    return df


# ----------------------------------------------------------------------------
# 3. Feature engineering (all strictly <= t; target is t+1)
# ----------------------------------------------------------------------------

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["grid_id", "hour_index"]).copy()
    g = df.groupby("grid_id", sort=False)

    df["internet_lag1"] = g["internet"].shift(0)  
    df["internet_lag2"] = g["internet"].shift(1)  
    df["internet_lag3"] = g["internet"].shift(2)  
    df["internet_lag24"] = g["internet"].shift(23) 

    roll3 = g["internet"].rolling(window=3, min_periods=3).agg(["mean", "std"])
    roll24 = g["internet"].rolling(window=24, min_periods=24).agg(["mean", "std"])
    roll3.index = roll3.index.droplevel(0)
    roll24.index = roll24.index.droplevel(0)
    df["internet_roll_mean_3"] = roll3["mean"]
    df["internet_roll_std_3"] = roll3["std"]
    df["internet_roll_mean_24"] = roll24["mean"]
    df["internet_roll_std_24"] = roll24["std"]

    df["calls_lag1"] = g["total_calls"].shift(0)
    df["sms_lag1"] = g["total_sms"].shift(0)

    df["internet_target"] = g["internet"].shift(-1)
    df["target_hour_index"] = df["hour_index"] + 1

    target_hour_of_day = df["target_hour_index"] % 24
    target_day_idx = df["target_hour_index"] // 24
    df["hour_sin"] = np.sin(2 * np.pi * target_hour_of_day / 24)
    df["hour_cos"] = np.cos(2 * np.pi * target_hour_of_day / 24)
    
    dow0 = df.loc[df["day_idx"] == 0, "day_of_week"].iloc[0]
    target_dow = (dow0 + target_day_idx) % 7
    df["dow_sin"] = np.sin(2 * np.pi * target_dow / 7)
    df["dow_cos"] = np.cos(2 * np.pi * target_dow / 7)

    return df


def assign_cell_clusters(df: pd.DataFrame, train_dates: set, n_clusters: int) -> pd.DataFrame:
    train_df = df[df["date"].isin(train_dates)]
    profile = (
        train_df.groupby(["grid_id", "hour"])["internet"]
        .mean()
        .unstack("hour")
        .fillna(0.0)
    )
    row_norm = profile.div(profile.sum(axis=1).replace(0, 1), axis=0)

    km = KMeans(n_clusters=n_clusters, random_state=RANDOM_STATE, n_init=10)
    labels = km.fit_predict(row_norm.values)
    cluster_map = pd.Series(labels, index=row_norm.index, name="cell_cluster")

    df = df.merge(cluster_map, left_on="grid_id", right_index=True, how="left")
    df["cell_cluster"] = df["cell_cluster"].fillna(-1).astype(int)
    return df


# ----------------------------------------------------------------------------
# 4. Assemble full feature table
# ----------------------------------------------------------------------------

def build_feature_table() -> pd.DataFrame:
    print("Loading CSVs...")
    df = load_all_days(CSV_GLOB)

    print("Building spatial adjacency from geojson...")
    adjacency = build_adjacency(GEOJSON_PATH)
    df = add_neighbor_feature(df, adjacency)

    print("Engineering temporal features...")
    df = engineer_features(df)

    print("Assigning cell clusters (fit on train days only)...")
    df = assign_cell_clusters(df, TRAIN_DATES, N_CLUSTERS)

    feature_cols = [
        "internet_lag1", "internet_lag2", "internet_lag3", "internet_lag24",
        "internet_roll_mean_3", "internet_roll_std_3",
        "internet_roll_mean_24", "internet_roll_std_24",
        "calls_lag1", "sms_lag1",
        "neighbor_mean_internet_t",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        "cell_cluster", "grid_id",
    ]
    required = feature_cols + ["internet_target"]
    df = df.dropna(subset=required).reset_index(drop=True)

    assert (df["target_hour_index"] - df["hour_index"] == 1).all(), \
        "Leak-safety check failed: target is not exactly one hour ahead of feature time."
    forbidden = {"total_activity", "sms_in", "sms_out", "call_in", "call_out"}
    assert forbidden.isdisjoint(feature_cols), \
        "Leak-safety check failed: a forbidden same-timestamp column leaked into features."

    return df, feature_cols


# ----------------------------------------------------------------------------
# 5. Train / validate / test split (by TARGET date, chronological)
# ----------------------------------------------------------------------------

def split_by_target_date(df: pd.DataFrame):
    day_lookup = sorted(df["date"].unique())
    df = df.copy()
    df["target_date"] = df["target_hour_index"].apply(
        lambda h: day_lookup[int(h // 24)] if h // 24 < len(day_lookup) else None
    )
    train = df[df["target_date"].isin(TRAIN_DATES)]
    val = df[df["target_date"].isin(VAL_DATES)]
    test = df[df["target_date"].isin(TEST_DATES)]
    return train, val, test


# ----------------------------------------------------------------------------
# 6. Train LightGBM
# ----------------------------------------------------------------------------

def train_model(train_df, val_df, feature_cols):
    cat_features = ["cell_cluster", "grid_id"]

    X_train = train_df[feature_cols]
    y_train = np.log1p(train_df["internet_target"])
    X_val = val_df[feature_cols]
    y_val = np.log1p(val_df["internet_target"])

    train_set = lgb.Dataset(X_train, label=y_train, categorical_feature=cat_features)
    val_set = lgb.Dataset(X_val, label=y_val, categorical_feature=cat_features, reference=train_set)

    params = {
        "objective": "regression",
        "metric": "mae",
        "learning_rate": 0.05,
        "num_leaves": 63,
        "min_data_in_leaf": 50,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 1,
        "seed": RANDOM_STATE,
        "verbose": -1,
    }

    model = lgb.train(
        params,
        train_set,
        num_boost_round=2000,
        valid_sets=[train_set, val_set],
        valid_names=["train", "val"],
        callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(period=100)],
    )
    return model


def evaluate(model, df, feature_cols, split_name: str):
    X = df[feature_cols]
    y_true = df["internet_target"].values
    y_pred = np.expm1(model.predict(X, num_iteration=model.best_iteration))
    y_pred = np.clip(y_pred, 0, None)

    mae = np.mean(np.abs(y_true - y_pred))
    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
    nonzero = y_true > 1e-6
    mape = np.mean(np.abs((y_true[nonzero] - y_pred[nonzero]) / y_true[nonzero])) * 100

    print(f"[{split_name}] MAE={mae:.3f}  RMSE={rmse:.3f}  MAPE={mape:.2f}%  (n={len(y_true)})")
    return y_pred


# ----------------------------------------------------------------------------
# 7. Anomaly detection: residual z-score with a strictly-past rolling baseline
# ----------------------------------------------------------------------------

def compute_anomaly_scores(df: pd.DataFrame, y_pred: np.ndarray, window: int = RESIDUAL_BASELINE_WINDOW):
    scored = df.copy()
    scored["y_pred"] = y_pred
    scored["residual"] = scored["internet_target"] - scored["y_pred"]
    scored = scored.sort_values(["grid_id", "target_hour_index"])

    g = scored.groupby("grid_id", sort=False)["residual"]
    past_residuals = g.shift(1)
    baseline_std = past_residuals.groupby(scored["grid_id"]).rolling(window, min_periods=window // 2).std()
    baseline_std.index = baseline_std.index.droplevel(0)

    scored["baseline_std"] = baseline_std
    scored["z_score"] = scored["residual"] / scored["baseline_std"].replace(0, np.nan)
    scored["is_anomaly"] = scored["z_score"].abs() > ANOMALY_Z_THRESHOLD

    return scored


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main():
    df, feature_cols = build_feature_table()
    print(f"Feature table: {df.shape[0]:,} rows, {len(feature_cols)} features")

    train_df, val_df, test_df = split_by_target_date(df)
    print(f"Train: {len(train_df):,}  Val: {len(val_df):,}  Test: {len(test_df):,}")

    model = train_model(train_df, val_df, feature_cols)

    evaluate(model, train_df, feature_cols, "train")
    evaluate(model, val_df, feature_cols, "val")
    test_pred = evaluate(model, test_df, feature_cols, "test")

    print("\nFeature importance (gain):")
    imp = pd.Series(model.feature_importance(importance_type="gain"), index=feature_cols)
    print(imp.sort_values(ascending=False).to_string())

    print("\nScoring anomalies on train+val+test (baseline needs history)...")
    full_pred = np.expm1(
        model.predict(pd.concat([train_df, val_df, test_df])[feature_cols], num_iteration=model.best_iteration)
    )
    full_pred = np.clip(full_pred, 0, None)
    scored = compute_anomaly_scores(pd.concat([train_df, val_df, test_df]), full_pred)

    n_anom = scored["is_anomaly"].sum()
    print(f"Flagged {n_anom:,} anomalous (cell, hour) points out of {len(scored):,}")

    top_anomalies = scored.reindex(scored["z_score"].abs().sort_values(ascending=False).index).head(20)
    print("\nTop 20 anomalies:")
    print(
        top_anomalies[
            ["grid_id", "target_date", "target_hour_index", "internet_target", "y_pred", "z_score"]
        ].to_string(index=False)
    )

    model.save_model("lgbm_internet_forecast.txt")
    scored.to_csv("anomaly_scores.csv", index=False)
    print("\nSaved model to lgbm_internet_forecast.txt and scores to anomaly_scores.csv")


if __name__ == "__main__":
    main()
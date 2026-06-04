import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, RANSACRegressor, HuberRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import math


train_raw = pd.read_csv("/run/determined/workdir/train.csv")
test_raw = pd.read_csv("/run/determined/workdir/train.csv")


train_raw['Intervallbeginn (UTC)'] = pd.to_datetime(train_raw['Intervallbeginn (UTC)'], utc=True)
train_raw['weekday'] = train_raw['Intervallbeginn (UTC)'].dt.weekday          # 0=Mon ... 6=Sun
train_raw['minute_of_day'] = train_raw['Intervallbeginn (UTC)'].dt.hour*60 + train_raw['Intervallbeginn (UTC)'].dt.minute
train_raw['bucket'] = (train_raw['minute_of_day'] // 60) * 60      # هر ۱۵ دقیقه


test_raw['Intervallbeginn (UTC)'] = pd.to_datetime(test_raw['Intervallbeginn (UTC)'], utc=True)
test_raw['weekday'] = test_raw['Intervallbeginn (UTC)'].dt.weekday          # 0=Mon ... 6=Sun
test_raw['minute_of_day'] = test_raw['Intervallbeginn (UTC)'].dt.hour*60 + test_raw['Intervallbeginn (UTC)'].dt.minute
test_raw['bucket'] = (test_raw['minute_of_day'] // 60) * 60      # هر ۱۵ دقیقه

# requirements: pandas, numpy, scikit-learn
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score
from xgboost import XGBRegressor

def fit_group_models(
    df_train: pd.DataFrame,
    x_col: str,
    y_col: str,
    group_cols = ["weekday", "bucket"],
    model_cls = XGBRegressor,
    model_kwargs: dict = None,
    min_samples_per_group: int = 30
):
    """
    Fit a model per group (group_cols). Returns:
      - models: dict mapping group_key -> fitted model
      - fallback: fitted global model (used when group lacks data)
      - stats: info about counts per group
    model_cls should have fit(X, y) and predict(X).
    """
    if model_kwargs is None:
        model_kwargs = {}

    df = df_train[[*group_cols, x_col, y_col]].copy()
    # drop rows with NaN in x or y
    df = df.dropna(subset=[x_col, y_col])
    # ensure numeric
    df[x_col] = pd.to_numeric(df[x_col], errors="coerce")
    df[y_col] = pd.to_numeric(df[y_col], errors="coerce")
    df = df.dropna(subset=[x_col, y_col])

    # group
    groups = df.groupby(group_cols)
    models = {}
    stats = {}

    # fit global fallback model (on all data)
    X_all = df[[x_col]].values.reshape(-1,1)
    y_all = df[y_col].values
    fallback = model_cls(**model_kwargs)
    fallback.fit(X_all, y_all)

    for gkey, gdf in groups:
        n = len(gdf)
        stats[gkey] = {"n": n}
        if n >= min_samples_per_group:
            model = model_cls(**model_kwargs)
            model.fit(gdf[[x_col]].values.reshape(-1,1), gdf[y_col].values)
            models[gkey] = model
            stats[gkey]["used_fallback"] = False
        else:
            # use fallback (but still store a marker)
            models[gkey] = None
            stats[gkey]["used_fallback"] = True

    return {"models": models, "fallback": fallback, "stats": stats}

def predict_with_group_models(df: pd.DataFrame, x_col: str, group_cols = ["weekday", "bucket"], models_package = None):
    """
    Apply group models to df. models_package is result of fit_group_models.
    Returns a copy of df with a new column 'y_pred' (predicted y).
    """
    df = df.copy()
    df['_pred'] = np.nan
    models = models_package["models"]
    fallback = models_package["fallback"]

    # ensure x numeric
    df[x_col] = pd.to_numeric(df[x_col], errors="coerce")

    # For rows with NaN x: leave NaN
    mask_valid = df[x_col].notna()

    for idx, row in df[mask_valid].iterrows():
        key = tuple(row[c] for c in group_cols)
        xval = np.array([[row[x_col]]], dtype=float)
        model = models.get(key, None)
        if model is None:
            # fallback
            yhat = fallback.predict(xval)[0]
        else:
            yhat = model.predict(xval)[0]
        df.at[idx, '_pred'] = yhat

    df = df.rename(columns={'_pred': 'y_pred'})
    return df

def evaluate_predictions(df: pd.DataFrame, y_true_col: str, y_pred_col: str = "y_pred"):
    df_eval = df.dropna(subset=[y_true_col, y_pred_col])
    if len(df_eval)==0:
        return {}
    y_true = df_eval[y_true_col].values
    y_pred = df_eval[y_pred_col].values
    mae = mean_absolute_error(y_true, y_pred)
    # MAPE: avoid division by zero by excluding zero true values or using small eps
    eps = 1e-6
    nonzero_mask = np.abs(y_true) > 0
    if nonzero_mask.sum() > 0:
        mape = (np.abs((y_true[nonzero_mask] - y_pred[nonzero_mask]) / y_true[nonzero_mask])).mean() * 100.0
    else:
        mape = np.nan
    r2 = r2_score(y_true, y_pred)
    return {"MAE": mae, "MAPE_percent_on_nonzero": mape, "R2": r2, "n": len(df_eval)}

# -------------------------
# Example usage for your columns:
# D91 = loop count (target y)
# V92 = video count (feature x)  — adapt names as you need
# train_raw, test_raw loaded and preprocessed (weekday, bucket computed)
# -------------------------
if __name__ == "__main__":
    # example: load train_raw / test_raw earlier
    # train_raw = pd.read_csv("train.csv"); test_raw = pd.read_csv("test.csv")
    # assume you already computed weekday and bucket on both
    x_col = "V92 (Belegungen/Intervall)"   # video detector counts (example)
    y_col = "D91 (Belegungen/Intervall)"   # loop detector counts (example)

    # fit group models
    package = fit_group_models(train_raw, x_col=x_col, y_col=y_col,
                               group_cols=["weekday","bucket"],
                               model_cls=LinearRegression,
                               model_kwargs={}, min_samples_per_group=30)

    # predict on test
    pred_df = predict_with_group_models(test_raw, x_col=x_col, group_cols=["weekday","bucket"], models_package=package)

    # evaluate
    metrics = evaluate_predictions(pred_df, y_true_col=y_col, y_pred_col="y_pred")
    print("Metrics:", metrics)

    # some diagnostics: how many groups used fallback?
    stats = package["stats"]
    total_groups = len(stats)
    fallback_used = sum(1 for v in stats.values() if v["used_fallback"])
    print(f"Groups total: {total_groups}, fallback used for {fallback_used} groups")

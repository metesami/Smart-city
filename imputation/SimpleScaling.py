import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression, RANSACRegressor, HuberRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import math

def prepare_minute_df(df, ts_col="Intervallbeginn (UTC)"):
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors='coerce')
    df = df.set_index(ts_col)
    # ensure strictly 1-minute frequency (fill missing minutes with NaNs)
    df = df.sort_index().resample('1T').asfreq()
    return df

def fit_linear_mapping(train_df, loop_col, video_col,
                       intercept=True, robust=None):
    """
    train_df: DataFrame indexed by minute, must contain loop_col and video_col
    intercept: bool, include intercept or force through zero
    robust: None | "ransac" | "huber"
    Returns: model object (sklearn-like), coef, intercept
    """
    # use only rows where both are present (and loop not NaN)
    mask = train_df[[loop_col, video_col]].notna().all(axis=1)
    X = train_df.loc[mask, video_col].values.reshape(-1,1)
    y = train_df.loc[mask, loop_col].values.reshape(-1,1)
    if len(X) < 10:
        raise ValueError("Not enough paired samples to fit (need >=10).")
    if robust == "ransac":
        base = LinearRegression(fit_intercept=intercept)
        model = RANSACRegressor(base_estimator=base, min_samples=0.5, residual_threshold=1.0, random_state=0)
    elif robust == "huber":
        model = HuberRegressor(fit_intercept=intercept)
    else:
        model = LinearRegression(fit_intercept=intercept)
    model.fit(X, y.ravel())
    coef = float(model.coef_.ravel()[0]) if hasattr(model, "coef_") else float(model.estimator_.coef_.ravel()[0])
    inter = float(model.intercept_) if hasattr(model, "intercept_") else float(model.estimator_.intercept_)
    return model, coef, inter

def apply_mapping_and_evaluate(model, test_df, loop_col, video_col, clip_negative=True):
    mask_video = test_df[video_col].notna()
    X_test = test_df.loc[mask_video, video_col].values.reshape(-1,1)
    y_pred = model.predict(X_test).ravel()
    if clip_negative:
        y_pred = np.clip(y_pred, 0, None)
    # assemble result series aligned to index
    pred_series = pd.Series(index=test_df.index[mask_video], data=y_pred)
    # metrics only where loop exists to compare
    mask_eval = test_df[[loop_col]].notna().iloc[mask_video.values].squeeze()
    if mask_eval.sum() > 0:
        y_true = test_df.loc[mask_video, loop_col].loc[mask_eval].values
        y_est  = y_pred[mask_eval.values]
        mae = mean_absolute_error(y_true, y_est)
        rmse = math.sqrt(mean_squared_error(y_true, y_est))
        r2 = r2_score(y_true, y_est) if len(y_true)>1 else float('nan')
    else:
        mae = rmse = r2 = float('nan')
    return pred_series, {"mae":mae, "rmse":rmse, "r2":r2, "n_eval":int(mask_eval.sum())}

# Example usage:
df_raw = pd.read_csv("/run/determined/workdir/train.csv")
test_raw = pd.read_csv("/run/determined/workdir/train.csv")
train_df = prepare_minute_df(df_raw)
test_df = prepare_minute_df(test_raw)
# choose columns for one lane: 
loop_col="D91 (Belegungen/Intervall)"
video_col="V92 (Belegungen/Intervall)"
# split train/test by time, e.g. train up to 2022-01-01, test after
# train_df = df_min.loc[:'2021-12-31']
# test_df  = df_min.loc['2022-01-01':]

model, coef, inter = fit_linear_mapping(train_df, loop_col, video_col, intercept=True, robust="ransac")
preds, stats = apply_mapping_and_evaluate(model, test_df, loop_col, video_col)
print(coef, inter, stats)

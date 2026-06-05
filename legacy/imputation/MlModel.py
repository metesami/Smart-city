
# file: sensor_ml_pipeline.py
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
import warnings
warnings.filterwarnings("ignore")

# ---------- helper: time features ----------
def add_time_features(df, ts_col="Intervallbeginn (UTC)"):
    df = df.copy()
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True)
    df["weekday"] = df[ts_col].dt.weekday  # 0=Mon .. 6=Sun
    df["minute_of_day"] = df[ts_col].dt.hour * 60 + df[ts_col].dt.minute
    # cyclical encoding for time-of-day (minutes)
    df["tod_sin"] = np.sin(2 * np.pi * df["minute_of_day"] / (24*60))
    df["tod_cos"] = np.cos(2 * np.pi * df["minute_of_day"] / (24*60))
    # weekday one-hot (optional) - we will keep as categorical int (user can one-hot if desired)
    # also keep bucket if needed
    return df

# ---------- feature builder ----------
def build_features(df, sensors, target_sensor="D91", keep_dwell=True):
    """
    df: DataFrame with columns like 'D91 (Belegungen/Intervall)', 'D91 (Verweilzeit/Intervall) [ms]', etc.
    sensors: list of sensor prefixes to include as features, eg ['D91','V92','V84','D82']
    target_sensor: prefix of loop sensor to predict (count)
    keep_dwell: if True include dwell columns as features
    returns X, y, feature_names
    """
    df = df.copy()
    # normalize column names (strip spaces)
    # map expected column patterns:
    def cnt_col(s): return f"{s}_count"
    def dwl_col(s): return f"{s}_dwell"

    # time features
    df = add_time_features(df)

    features = []
    for s in sensors:
        # counts
        if cnt_col(s) in df.columns:
            col = cnt_col(s)
            features.append(col)
        else:
            # if missing sensor column, create NaNs
            df[cnt_col(s)] = np.nan
            features.append(cnt_col(s))
        # dwells
        if keep_dwell:
            if dwl_col(s) in df.columns:
                features.append(dwl_col(s))
            else:
                df[dwl_col(s)] = np.nan
                features.append(dwl_col(s))

    # include time features
    features += ["tod_sin", "tod_cos", "weekday"]

    # target
    target_col = cnt_col(target_sensor)
    if target_col not in df.columns:
        raise ValueError(f"Target column {target_col} not in dataframe")

    # drop rows where target is NaN (can't train)
    mask = ~df[target_col].isna()
    df = df.loc[mask]

    X = df[features].astype(float).fillna(0.0)  # simple imputation (0). you can choose different strategy
    y = df[target_col].astype(float)

    return X, y, features

# ---------- model factory ----------
def get_model(name="ridge", random_state=42):
    """
    name: 'ridge', 'rf', 'xgb', 'lstm'
    returns: (model_object, is_sequence_model)
    For sklearn/xgboost models return a fit/predict API.
    For 'lstm' we will handle separately (Keras).
    """
    name = name.lower()
    if name == "ridge":
        from sklearn.linear_model import Ridge
        model = Pipeline([("scaler", StandardScaler()), ("m", Ridge(alpha=1.0, random_state=random_state))])
        return model, False
    if name == "rf":
        from sklearn.ensemble import RandomForestRegressor
        model = Pipeline([("scaler", StandardScaler()), ("m", RandomForestRegressor(n_estimators=100, random_state=random_state, n_jobs=-1))])
        return model, False
    if name == "xgb":
        try:
            from xgboost import XGBRegressor
        except Exception as e:
            raise ImportError("xgboost not installed. pip install xgboost") from e
        model = XGBRegressor(n_estimators=200, objective="reg:squarederror", random_state=random_state, n_jobs=-1)
        return model, False
    if name == "lstm":
        # LSTM uses sequences; we'll return None here and handle separately
        return None, True
    raise ValueError("Unknown model name")

# ---------- train/eval generic (non-sequence) ----------
def train_eval_model(X_train, y_train, X_test, y_test, model_name="ridge"):
    model, is_seq = get_model(model_name)
    if is_seq:
        raise ValueError("Sequence model requested but train_eval_model is for tabular models")
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = {
        "MAE": mean_absolute_error(y_test, preds),
        "R2": r2_score(y_test, preds),
        "n": len(y_test)
    }
    return model, preds, metrics

# ---------- LSTM pipeline (sequence) ----------
def make_sequences(X, y, lookback=12, step=1):
    """
    Convert tabular X (num_samples x num_features) into sequences for LSTM.
    lookback: number of time steps (e.g., 12 for last 12 minutes)
    Returns X_seq (N,y,features) and y_aligned
    Note: assumes rows are in time order.
    """
    Xv = X.values
    yv = y.values
    n = len(Xv)
    seqs = []
    ys = []
    for i in range(lookback, n):
        seqs.append(Xv[i-lookback:i:step])
        ys.append(yv[i])
    return np.stack(seqs), np.array(ys)

def build_and_train_lstm(X_train, y_train, X_val, y_val, epochs=10, batch_size=1024, lookback=12):
    try:
        import tensorflow as tf
        from tensorflow.keras.models import Sequential
        from tensorflow.keras.layers import LSTM, Dense, Dropout
        from tensorflow.keras.callbacks import EarlyStopping
    except Exception as e:
        raise ImportError("tensorflow not installed. pip install tensorflow") from e

    # scale features
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s = scaler.transform(X_val)

    # make sequences
    Xtr_seq, ytr_seq = make_sequences(pd.DataFrame(X_train_s), y_train, lookback=lookback)
    Xv_seq, yv_seq = make_sequences(pd.DataFrame(X_val_s), y_val, lookback=lookback)

    n_features = Xtr_seq.shape[2]

    model = Sequential([
        LSTM(64, input_shape=(Xtr_seq.shape[1], n_features), return_sequences=False),
        Dropout(0.2),
        Dense(32, activation="relu"),
        Dense(1)
    ])
    model.compile(optimizer="adam", loss="mse", metrics=["mae"])
    es = EarlyStopping(monitor="val_loss", patience=3, restore_best_weights=True)
    model.fit(Xtr_seq, ytr_seq, validation_data=(Xv_seq, yv_seq),
              epochs=epochs, batch_size=batch_size, callbacks=[es], verbose=1)
    preds_val = model.predict(Xv_seq).ravel()
    metrics = {
        "MAE": mean_absolute_error(yv_seq, preds_val),
        "R2": r2_score(yv_seq, preds_val),
        "n": len(yv_seq)
    }
    return model, scaler, metrics

# ---------- example orchestrator ----------
def run_pipeline(train_path, test_path,
                 sensors=["D91","V92","V84","D82"],
                 target_sensor="D91",
                 model_name="ridge",
                 keep_dwell=True):
    train_raw = pd.read_csv(train_path)
    test_raw = pd.read_csv(test_path)

    X_train, y_train, feat_names = build_features(train_raw, sensors, target_sensor, keep_dwell=keep_dwell)
    X_test, y_test, _ = build_features(test_raw, sensors, target_sensor, keep_dwell=keep_dwell)

    # simple split if you want validation from train set
    # Xtr, Xval, ytr, yval = train_test_split(X_train, y_train, test_size=0.1, random_state=42, shuffle=False)
    # but we'll train on X_train and evaluate on X_test as user wanted
    if model_name.lower() == "lstm":
        # train LSTM - careful with sizes
        model, scaler, metrics = build_and_train_lstm(X_train, y_train, X_test, y_test, epochs=10, batch_size=1024, lookback=12)
        print("LSTM metrics:", metrics)
        return model, scaler, feat_names, metrics
    else:
        model, preds, metrics = train_eval_model(X_train, y_train, X_test, y_test, model_name=model_name)
        print("Model:", model_name, "Metrics:", metrics)
        # save model
        joblib.dump((model, feat_names), f"model_{model_name}.joblib")
        return model, feat_names, metrics

# ----------------- usage example -----------------
if __name__ == "__main__":
    # change paths to your train/test csv paths
    TRAIN = "/run/determined/workdir/train.csv"
    TEST = "/run/determined/workdir/test.csv"
    # sensors that will be used as predictors (counts + dwells)
    SENSORS = ["V92","V84","D82"]
    # target is loop D91
    run_pipeline(TRAIN, TEST, sensors=SENSORS, target_sensor="D91", model_name="xgb", keep_dwell=True)
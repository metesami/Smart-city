# V1
'''
In V1, short realistic gaps were artificially introduced 
into high-confidence observed data, allowing direct comparison 
between imputed values and known ground truth.
'''

#test just on some sensors and an specific periode 
SENSORS_V1 = ["D12", "D13", "V02"]
START = "2022-05-01"
END   = "2023-07-07"


df_base = df_imp[

    (df_clean["missing_reason"] == "NONE")
].copy()

df_base["count_true"] = df_base["count_clean"]
df_base["dwell_true"] = df_base["dwell_clean"]

import numpy as np

np.random.seed(42)

def inject_zero_run_short_fast(g, n_segments=3, seg_len=3):
    g = g.copy().reset_index(drop=True)  # 👈 بسیار مهم

    n = len(g)
    if n < seg_len + 5:
        return g

    possible_starts = np.arange(0, n - seg_len)
    starts = np.random.choice(
        possible_starts,
        size=min(n_segments, n // 10),
        replace=False
    )

    for s in starts:
        seg = range(s, s + seg_len)
        g.loc[seg, "count_clean"] = np.nan
        g.loc[seg, "dwell_clean"] = np.nan
        g.loc[seg, "missing_reason"] = "ZERO_RUN_SHORT"

    return g



def impute_v1_only(g):
    mask = g["missing_reason"] == "ZERO_RUN_SHORT"

    g.loc[mask, "count_imputed"] = (
        g["count_clean"]
        .interpolate(method="linear", limit=5, limit_direction="both")
    )

    g.loc[mask, "dwell_imputed"] = (
        g["dwell_clean"]
        .interpolate(method="linear", limit=5, limit_direction="both")
    )

    return g

out = []

for sid, g in df_base.groupby("sensor_id"):
    g = g.sort_values("timestamp")
    g = inject_zero_run_short_fast(g)
    g["count_imputed"] = g["count_clean"]
    g["dwell_imputed"] = g["dwell_clean"]
    g = impute_v1_only(g)
    out.append(g)

v1 = pd.concat(out)

eval_v1 = v1[
    (v1["missing_reason"] == "ZERO_RUN_SHORT") &
    (v1["count_imputed"].notna())
]

mae = np.mean(np.abs(eval_v1["count_imputed"] - eval_v1["count_true"]))
rmse = np.sqrt(np.mean((eval_v1["count_imputed"] - eval_v1["count_true"])**2))



# V2 
'''

'''

import re

MISSING_REASON_PRIORITY = [
    "PHYS_INVALID",
    "CAP_EXCEEDED",
    "STUCK_OFF",
    "STUCK_ON",
    "SPIKE",
    "CLIFF",
    "PROFILE_HARD",
    "ZERO_RUN_LONG",
    "ZERO_RUN_SHORT",
    "PROFILE_SOFT",
    "LOGIC_INVALID",
    "NONE",
]

IMPUTABLE_REASONS = {
    "ZERO_RUN_SHORT",
    "PROFILE_SOFT",
    "SPIKE",
}

CONFIG = {
    "timestamp_candidates": [
        "Intervallbeginn (UTC)",
        "Intervallbeginn",
        "timestamp",
        "time",
        "datetime",
    ],
    "minute_seconds": 60,
    "avg_dwell_min_ms": 300,
    "avg_dwell_max_ms": 10000,
    "dwell_abs_max_ms": 60000,
    "global_cap_per_minute": 35,
    "cap_suspicious_lower": 30,
    "spike_mad_multiplier": 5.0,
    "spike_window_minutes": 60,
    "zero_run_soft_minutes": 5,
    "stuck_off_minutes": 20,
    "stuck_on_minutes": 30,
    "profile_group_minutes": 15,
    "profile_soft_z": 5.0,
    "profile_hard_z": 7.5,
    "stuck_on_mean": 3,
    "stuck_on_std": 1e-6,
}

COUNT_PAT = re.compile(r"^(?P<sid>[DV]\d+)\s*\(Belegungen/Intervall\)\s*$")
DWELL_PAT = re.compile(r"^(?P<sid>[DV]\d+)\s*\(Verweilzeit/Intervall\)\s*\[ms\]\s*$")
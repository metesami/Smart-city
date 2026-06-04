# pip install duckdb pandas

import duckdb
import json
import gzip
import urllib.parse
import datetime as dt
import pandas as pd
from datetime import timezone

# -----------------------------
# Config
# -----------------------------
IN_PARQUET = "/run/determined/workdir/traffic/intersection_imputed_layered_10min_analytics.parquet"
OUT_NT_GZ  = "/run/determined/workdir/traffic/A142_traffic_10min_abox.nt.gz"

SENSOR_MAP_JSON  = "/run/determined/workdir/traffic/sensor_uri_map.json"
SENSOR2LANE_JSON = "/run/determined/workdir/traffic/sensor_to_lane_map.json"

BATCH = 100_000
WINDOW_SEC = 600

# -----------------------------
# Namespaces / URIs
# -----------------------------
NS_EX      = "http://example.org/traffic/"
NS_SC      = "http://example.org/smartcity/core#"
NS_TRAFFIC = "http://example.org/smartcity/traffic#"
NS_SOSA    = "http://www.w3.org/ns/sosa/"
NS_TIME    = "http://www.w3.org/2006/time#"
NS_SCTIME  = "http://example.org/smartcity/time/"
NS_XSD     = "http://www.w3.org/2001/XMLSchema#"

URI_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

URI_SOSA_OBSERVATION       = f"<{NS_SOSA}Observation>"
URI_SOSA_OBSERVED_PROPERTY = f"<{NS_SOSA}observedProperty>"
URI_SOSA_HAS_SIMPLE_RESULT = f"<{NS_SOSA}hasSimpleResult>"
URI_SOSA_PHENOMENON_TIME   = f"<{NS_SOSA}phenomenonTime>"

URI_TIME_INSTANT         = f"<{NS_TIME}Instant>"
URI_TIME_IN_XSD_DATETIME = f"<{NS_TIME}inXSDDateTime>"

URI_TRAFFIC_MADE_BY_SENSOR       = f"<{NS_TRAFFIC}madeBySensor>"
URI_TRAFFIC_HAS_FEATURE_INTEREST = f"<{NS_TRAFFIC}hasFeatureOfInterest>"
URI_TRAFFIC_OBSERVATION          = f"<{NS_TRAFFIC}TrafficObservation>"
URI_TRAFFIC_VEHICLE_COUNT        = f"<{NS_TRAFFIC}VehicleCount>"
URI_TRAFFIC_OCCUPANCY_TIME       = f"<{NS_TRAFFIC}OccupancyTime>"
URI_TRAFFIC_CONF_MIN             = f"<{NS_TRAFFIC}confidenceMin>"
URI_TRAFFIC_CONF_MEAN            = f"<{NS_TRAFFIC}confidenceMean>"
URI_TRAFFIC_COVERAGE_COUNT       = f"<{NS_TRAFFIC}coverageCount>"
URI_TRAFFIC_COVERAGE_OCC         = f"<{NS_TRAFFIC}coverageOccupancy>"
URI_TRAFFIC_IMPUTED_RATE         = f"<{NS_TRAFFIC}imputedRate>"
URI_TRAFFIC_CLEAN_OBS_RATE       = f"<{NS_TRAFFIC}cleanObservedRate>"

URI_SC_OBSERVED_AT_TIMEINDEX = f"<{NS_SC}observedAtTimeIndex>"
URI_SC_AGG_WINDOW_SECONDS    = f"<{NS_SC}aggregationWindowSeconds>"

# -----------------------------
# Helpers
# -----------------------------
def u(uri: str) -> str:
    return f"<{uri}>"

def triple(s: str, p: str, o: str) -> str:
    return f"{s} {p} {o} .\n"

def safe_local(text: str) -> str:
    return urllib.parse.quote(str(text).strip(), safe="")

def lit_double(x) -> str:
    return f"\"{float(x)}\"^^<{NS_XSD}double>"

def lit_long(x) -> str:
    return f"\"{int(x)}\"^^<{NS_XSD}long>"

def lit_int(x) -> str:
    return f"\"{int(x)}\"^^<{NS_XSD}integer>"

def lit_datetime(x: str) -> str:
    return f"\"{x}\"^^<{NS_XSD}dateTime>"

def is_valid(x) -> bool:
    return pd.notna(x)

def epoch_to_iso(ts_seconds: int) -> str:
    return dt.datetime.fromtimestamp(int(ts_seconds), tz=dt.timezone.utc).isoformat()

def to_epoch_seconds(ts: pd.Timestamp) -> int:
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return int(ts.timestamp())

# -----------------------------
# Load maps
# -----------------------------
with open(SENSOR_MAP_JSON, "r", encoding="utf-8") as f:
    sensor_uri_map = json.load(f)

try:
    with open(SENSOR2LANE_JSON, "r", encoding="utf-8") as f:
        sensor_to_lane_map = json.load(f)
except Exception:
    sensor_to_lane_map = {}

print("Loaded maps:",
      "\n  sensors     :", len(sensor_uri_map),
      "\n  sensor→lane :", len(sensor_to_lane_map))

# -----------------------------
# DuckDB
# -----------------------------
con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=8;")

query = f"""
SELECT
  sensor_id,
  timestamp,
  count_10min,
  occupancy_time_10min,
  confidence_min,
  confidence_mean,
  coverage_count,
  coverage_dwell,
  imputed_rate,
  is_clean_observed_rate
FROM '{IN_PARQUET}'
WHERE sensor_id IS NOT NULL AND timestamp IS NOT NULL
"""

cur = con.execute(query)

# -----------------------------
# Write RDF stream directly
# -----------------------------
total_obs = 0
count_obs_minted = 0
occ_obs_minted = 0

rows_seen = 0
rows_skipped_empty_sid = 0
rows_skipped_missing_sensor_map = 0
rows_skipped_bad_timestamp = 0

time_inst_added = set()

with gzip.open(OUT_NT_GZ, "wt", encoding="utf-8") as fout:
    while True:
        rows = cur.fetchmany(BATCH)
        if not rows:
            break

        buf = []

        for (
            sid,
            ts,
            count_10min,
            occ_time,
            conf_min,
            conf_mean,
            coverage_count,
            coverage_dwell,
            imputed_rate,
            clean_obs_rate
        ) in rows:
            rows_seen += 1

            sid = str(sid).strip()
            if not sid:
                rows_skipped_empty_sid += 1
                continue

            sensor_uri_str = sensor_uri_map.get(sid)
            if not sensor_uri_str:
                rows_skipped_missing_sensor_map += 1
                continue
            sensor_uri = u(sensor_uri_str)

            lane_uri_str = sensor_to_lane_map.get(sid)
            lane_uri = u(lane_uri_str) if lane_uri_str else None

            ts = pd.to_datetime(ts, utc=True, errors="coerce")
            if pd.isna(ts):
                rows_skipped_bad_timestamp += 1
                continue

            t_idx = to_epoch_seconds(ts)
            iso_t = epoch_to_iso(t_idx)
            t_inst = u(f"{NS_SCTIME}t_{t_idx}")
            sid_safe = safe_local(sid)

            # ---------------------------
            # Shared time instant
            # ---------------------------
            if t_idx not in time_inst_added:
                buf.append(triple(t_inst, URI_RDF_TYPE, URI_TIME_INSTANT))
                buf.append(triple(t_inst, URI_TIME_IN_XSD_DATETIME, lit_datetime(iso_t)))
                time_inst_added.add(t_idx)

            # ---------------------------
            # VehicleCount observation
            # ---------------------------
            if is_valid(count_10min):
                obs_count = u(f"{NS_EX}obsCount_{sid_safe}_{t_idx}")

                buf.append(triple(obs_count, URI_RDF_TYPE, URI_SOSA_OBSERVATION))
                buf.append(triple(obs_count, URI_RDF_TYPE, URI_TRAFFIC_OBSERVATION))
                buf.append(triple(obs_count, URI_TRAFFIC_MADE_BY_SENSOR, sensor_uri))
                buf.append(triple(obs_count, URI_SOSA_OBSERVED_PROPERTY, URI_TRAFFIC_VEHICLE_COUNT))
                buf.append(triple(obs_count, URI_SOSA_HAS_SIMPLE_RESULT, lit_double(count_10min)))
                buf.append(triple(obs_count, URI_SOSA_PHENOMENON_TIME, t_inst))
                buf.append(triple(obs_count, URI_SC_OBSERVED_AT_TIMEINDEX, lit_long(t_idx)))
                buf.append(triple(obs_count, URI_SC_AGG_WINDOW_SECONDS, lit_int(WINDOW_SEC)))

                if lane_uri:
                    buf.append(triple(obs_count, URI_TRAFFIC_HAS_FEATURE_INTEREST, lane_uri))
                if is_valid(conf_min):
                    buf.append(triple(obs_count, URI_TRAFFIC_CONF_MIN, lit_double(conf_min)))
                if is_valid(conf_mean):
                    buf.append(triple(obs_count, URI_TRAFFIC_CONF_MEAN, lit_double(conf_mean)))
                if is_valid(coverage_count):
                    buf.append(triple(obs_count, URI_TRAFFIC_COVERAGE_COUNT, lit_double(coverage_count)))
                if is_valid(imputed_rate):
                    buf.append(triple(obs_count, URI_TRAFFIC_IMPUTED_RATE, lit_double(imputed_rate)))
                if is_valid(clean_obs_rate):
                    buf.append(triple(obs_count, URI_TRAFFIC_CLEAN_OBS_RATE, lit_double(clean_obs_rate)))

                total_obs += 1
                count_obs_minted += 1

            # ---------------------------
            # OccupancyTime observation
            # ---------------------------
            if is_valid(occ_time):
                obs_occ = u(f"{NS_EX}obsOcc_{sid_safe}_{t_idx}")

                buf.append(triple(obs_occ, URI_RDF_TYPE, URI_SOSA_OBSERVATION))
                buf.append(triple(obs_occ, URI_RDF_TYPE, URI_TRAFFIC_OBSERVATION))
                buf.append(triple(obs_occ, URI_TRAFFIC_MADE_BY_SENSOR, sensor_uri))
                buf.append(triple(obs_occ, URI_SOSA_OBSERVED_PROPERTY, URI_TRAFFIC_OCCUPANCY_TIME))
                buf.append(triple(obs_occ, URI_SOSA_HAS_SIMPLE_RESULT, lit_double(occ_time)))
                buf.append(triple(obs_occ, URI_SOSA_PHENOMENON_TIME, t_inst))
                buf.append(triple(obs_occ, URI_SC_OBSERVED_AT_TIMEINDEX, lit_long(t_idx)))
                buf.append(triple(obs_occ, URI_SC_AGG_WINDOW_SECONDS, lit_int(WINDOW_SEC)))

                if lane_uri:
                    buf.append(triple(obs_occ, URI_TRAFFIC_HAS_FEATURE_INTEREST, lane_uri))
                if is_valid(conf_min):
                    buf.append(triple(obs_occ, URI_TRAFFIC_CONF_MIN, lit_double(conf_min)))
                if is_valid(conf_mean):
                    buf.append(triple(obs_occ, URI_TRAFFIC_CONF_MEAN, lit_double(conf_mean)))
                if is_valid(coverage_dwell):
                    buf.append(triple(obs_occ, URI_TRAFFIC_COVERAGE_OCC, lit_double(coverage_dwell)))
                if is_valid(imputed_rate):
                    buf.append(triple(obs_occ, URI_TRAFFIC_IMPUTED_RATE, lit_double(imputed_rate)))
                if is_valid(clean_obs_rate):
                    buf.append(triple(obs_occ, URI_TRAFFIC_CLEAN_OBS_RATE, lit_double(clean_obs_rate)))

                total_obs += 1
                occ_obs_minted += 1

        fout.write("".join(buf))

print("Total observations minted (count+occupancy):", total_obs)
print("  - VehicleCount observations :", count_obs_minted)
print("  - OccupancyTime observations:", occ_obs_minted)
print("Rows seen                     :", rows_seen)
print("Rows skipped (empty sid)      :", rows_skipped_empty_sid)
print("Rows skipped (sensor not map) :", rows_skipped_missing_sensor_map)
print("Rows skipped (bad timestamp)  :", rows_skipped_bad_timestamp)
print("Unique time instants minted   :", len(time_inst_added))
print("✅ RDF stream saved:", OUT_NT_GZ)
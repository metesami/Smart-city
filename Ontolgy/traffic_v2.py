# pip install rdflib duckdb pandas

import duckdb
import json, urllib.parse
from datetime import timezone
import pandas as pd

from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD

# --- Namespaces ---
EX      = Namespace("http://example.org/traffic/")
SC      = Namespace("http://example.org/smartcity/core#")
TRAFFIC = Namespace("http://example.org/smartcity/traffic#")
SOSA    = Namespace("http://www.w3.org/ns/sosa/")
TIME    = Namespace("http://www.w3.org/2006/time#")

# --- Paths ---
IN_PARQUET = "/content/drive/MyDrive/Smart-city/intersection_imputed_layered_10min_analytics.parquet"
OUT_TTL    = "/content/drive/MyDrive/Smart-city/A142_traffic_10min_abox.ttl"

# URI maps written by intersection_metadata.py
SENSOR_MAP_JSON  = "/content/drive/MyDrive/Smart-city/sensor_uri_map.json"
SENSOR2LANE_JSON = "/content/drive/MyDrive/Smart-city/sensor_to_lane_map.json"

sensor_uri_map = json.load(open(SENSOR_MAP_JSON))
try:
    sensor_to_lane_map = json.load(open(SENSOR2LANE_JSON))
except Exception:
    sensor_to_lane_map = {}

print("Loaded maps:",
      "\n  sensors     :", len(sensor_uri_map),
      "\n  sensor→lane :", len(sensor_to_lane_map))

# --- RDF graph ---
g = Graph()
g.bind("ex", EX)
g.bind("sc", SC)
g.bind("traffic", TRAFFIC)
g.bind("sosa", SOSA)
g.bind("time", TIME)

# avoid re-adding same Instant nodes
time_inst_added = set()

def to_epoch_seconds(ts: pd.Timestamp) -> int:
    # ensure UTC
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return int(ts.timestamp())

# --- DuckDB scan (fast) ---
con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=8;")

# We only select what we need to mint observations
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

BATCH = 20000  # tune if needed
total_obs = 0

while True:
    rows = cur.fetchmany(BATCH)
    if not rows:
        break

    # rows are tuples in the same order as query columns
    for (sid, ts, count_10min, occ_time, conf_min, conf_mean,
         cov_c, cov_o, imputed_rate, obs_rate) in rows:

        sid = str(sid).strip()
        sensor_uri_str = sensor_uri_map.get(sid)
        if not sensor_uri_str:
            continue
        sensor_uri = URIRef(sensor_uri_str)

        # lane (feature of interest) if available
        lane_uri_str = sensor_to_lane_map.get(sid)
        lane_uri = URIRef(lane_uri_str) if lane_uri_str else None

        # timestamp handling
        ts = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(ts):
            continue

        iso_t = ts.isoformat()
        t_key = urllib.parse.quote_plus(iso_t)

        # time:Instant node
        t_inst = EX[f"t_{t_key}"]
        if t_key not in time_inst_added:
            g.add((t_inst, RDF.type, TIME.Instant))
            g.add((t_inst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(t_key)

        # numeric time index for ATiSE / TKGE
        t_idx = to_epoch_seconds(ts)

        # ---- Observation: VehicleCount (only if non-null)
        if count_10min is not None:
            obs_count = EX[f"obsCount_{sid}_{t_key}"]
            g.add((obs_count, RDF.type, SOSA.Observation))
            g.add((obs_count, RDF.type, TRAFFIC.TrafficObservation))
            g.add((obs_count, SOSA.madeBySensor, sensor_uri))
            g.add((obs_count, SOSA.observedProperty, TRAFFIC.VehicleCount))
            g.add((obs_count, SOSA.hasSimpleResult, Literal(float(count_10min), datatype=XSD.double)))
            g.add((obs_count, SOSA.phenomenonTime, t_inst))
            g.add((obs_count, SC.observedAtTimeIndex, Literal(t_idx, datatype=XSD.long)))
            if lane_uri:
                g.add((obs_count, SOSA.hasFeatureOfInterest, lane_uri))

            # minimal quality annotations (optional but useful; stable columns)
            if conf_min is not None:
                g.add((obs_count, TRAFFIC.confidenceMin, Literal(float(conf_min), datatype=XSD.double)))
            if conf_mean is not None:
                g.add((obs_count, TRAFFIC.confidenceMean, Literal(float(conf_mean), datatype=XSD.double)))
            if cov_c is not None:
                g.add((obs_count, TRAFFIC.coverageCount, Literal(float(cov_c), datatype=XSD.double)))
            if imputed_rate is not None:
                g.add((obs_count, TRAFFIC.imputedRate, Literal(float(imputed_rate), datatype=XSD.double)))
            if obs_rate is not None:
                g.add((obs_count, TRAFFIC.cleanObservedRate, Literal(float(obs_rate), datatype=XSD.double)))

            total_obs += 1

        # ---- Observation: OccupancyTime (only if non-null)
        if occ_time is not None:
            obs_occ = EX[f"obsOcc_{sid}_{t_key}"]
            g.add((obs_occ, RDF.type, SOSA.Observation))
            g.add((obs_occ, RDF.type, TRAFFIC.TrafficObservation))
            g.add((obs_occ, SOSA.madeBySensor, sensor_uri))
            g.add((obs_occ, SOSA.observedProperty, TRAFFIC.OccupancyTime))  # ✅ fixed meaning
            g.add((obs_occ, SOSA.hasSimpleResult, Literal(float(occ_time), datatype=XSD.double)))
            g.add((obs_occ, SOSA.phenomenonTime, t_inst))
            g.add((obs_occ, SC.observedAtTimeIndex, Literal(t_idx, datatype=XSD.long)))
            if lane_uri:
                g.add((obs_occ, SOSA.hasFeatureOfInterest, lane_uri))

            # occupancy coverage/quality
            if conf_min is not None:
                g.add((obs_occ, TRAFFIC.confidenceMin, Literal(float(conf_min), datatype=XSD.double)))
            if conf_mean is not None:
                g.add((obs_occ, TRAFFIC.confidenceMean, Literal(float(conf_mean), datatype=XSD.double)))
            if cov_o is not None:
                g.add((obs_occ, TRAFFIC.coverageOccupancy, Literal(float(cov_o), datatype=XSD.double)))
            if imputed_rate is not None:
                g.add((obs_occ, TRAFFIC.imputedRate, Literal(float(imputed_rate), datatype=XSD.double)))
            if obs_rate is not None:
                g.add((obs_occ, TRAFFIC.cleanObservedRate, Literal(float(obs_rate), datatype=XSD.double)))

            total_obs += 1

print("Total observations minted (count+occupancy):", total_obs)

# --- Save ---
g.serialize(destination=OUT_TTL, format="turtle")
print("✅ RDF saved:", OUT_TTL, "triples:", len(g))

# pip install rdflib duckdb pandas

import duckdb
import json, urllib.parse
from datetime import timezone
import pandas as pd
import datetime as dt

from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD

EX      = Namespace("http://example.org/traffic/")
SC      = Namespace("http://example.org/smartcity/core#")
TRAFFIC = Namespace("http://example.org/smartcity/traffic#")
SOSA    = Namespace("http://www.w3.org/ns/sosa/")
TIME    = Namespace("http://www.w3.org/2006/time#")
SCTIME = Namespace("http://example.org/smartcity/time/")


IN_PARQUET = "/content/drive/MyDrive/Smart-city/intersection_imputed_layered_10min_analytics.parquet"
OUT_TTL    = "/content/drive/MyDrive/Smart-city/A142_traffic_10min_abox.ttl"

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

g = Graph()
g.bind("ex", EX)
g.bind("sc", SC)
g.bind("traffic", TRAFFIC)
g.bind("sosa", SOSA)
g.bind("time", TIME)
g.bind("sctime", SCTIME)

time_inst_added = set()


def epoch_to_iso(ts_seconds: int) -> str:
    return dt.datetime.fromtimestamp(int(ts_seconds), tz=dt.timezone.utc).isoformat()

def to_epoch_seconds(ts: pd.Timestamp) -> int:
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return int(ts.timestamp())

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

# instant for all
WINDOW_SEC = 600

BATCH = 20000
total_obs = 0

while True:
    rows = cur.fetchmany(BATCH)
    if not rows:
        break

    for (sid, ts, count_10min, occ_time, conf_min, conf_mean,
         cov_c, cov_o, imputed_rate, obs_rate) in rows:

        sid = str(sid).strip()
        sensor_uri_str = sensor_uri_map.get(sid)
        if not sensor_uri_str:
            continue
        sensor_uri = URIRef(sensor_uri_str)

        lane_uri_str = sensor_to_lane_map.get(sid)
        lane_uri = URIRef(lane_uri_str) if lane_uri_str else None

        ts = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(ts):
            continue

        t_idx = to_epoch_seconds(ts)
        iso_t = epoch_to_iso(t_idx)

        t_inst = SCTIME[f"t_{t_idx}"]
        if t_idx not in time_inst_added:
            g.add((t_inst, RDF.type, TIME.Instant))
            g.add((t_inst, TIME.inXSDDateTime,
                Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(t_idx)




        # VehicleCount
        if pd.notna(count_10min):
            obs_count = EX[f"obsCount_{sid}_{t_idx}"]
            g.add((obs_count, RDF.type, SOSA.Observation))
            g.add((obs_count, RDF.type, TRAFFIC.TrafficObservation))
            g.add((obs_count, SOSA.madeBySensor, sensor_uri))
            g.add((obs_count, SOSA.observedProperty, TRAFFIC.VehicleCount))
            g.add((obs_count, SOSA.hasSimpleResult, Literal(float(count_10min), datatype=XSD.double)))
            g.add((obs_count, SOSA.phenomenonTime, t_inst))
            g.add((obs_count, SC.observedAtTimeIndex, Literal(t_idx, datatype=XSD.long)))
            g.add((obs_count, SC.aggregationWindowSeconds, Literal(600, datatype=XSD.integer)))

            if lane_uri:
                g.add((obs_count, SOSA.hasFeatureOfInterest, lane_uri))

            if pd.notna(conf_min):
                g.add((obs_count, TRAFFIC.confidenceMin, Literal(float(conf_min), datatype=XSD.double)))
            if pd.notna(conf_mean):
                g.add((obs_count, TRAFFIC.confidenceMean, Literal(float(conf_mean), datatype=XSD.double)))
            if pd.notna(cov_c):
                g.add((obs_count, TRAFFIC.coverageCount, Literal(float(cov_c), datatype=XSD.double)))
            if pd.notna(imputed_rate):
                g.add((obs_count, TRAFFIC.imputedRate, Literal(float(imputed_rate), datatype=XSD.double)))
            if pd.notna(obs_rate):
                g.add((obs_count, TRAFFIC.cleanObservedRate, Literal(float(obs_rate), datatype=XSD.double)))

            total_obs += 1

        # OccupancyTime
        if pd.notna(occ_time):
            obs_occ = EX[f"obsOcc_{sid}_{t_idx}"]
            g.add((obs_occ, RDF.type, SOSA.Observation))
            g.add((obs_occ, RDF.type, TRAFFIC.TrafficObservation))
            g.add((obs_occ, SOSA.madeBySensor, sensor_uri))
            g.add((obs_occ, SOSA.observedProperty, TRAFFIC.OccupancyTime))
            g.add((obs_occ, SOSA.hasSimpleResult, Literal(float(occ_time), datatype=XSD.double)))
            g.add((obs_occ, SOSA.phenomenonTime, t_inst))
            g.add((obs_occ, SC.observedAtTimeIndex, Literal(t_idx, datatype=XSD.long)))
            g.add((obs_occ, SC.aggregationWindowSeconds, Literal(600, datatype=XSD.integer)))

            if lane_uri:
                g.add((obs_occ, SOSA.hasFeatureOfInterest, lane_uri))

            if pd.notna(conf_min):
                g.add((obs_occ, TRAFFIC.confidenceMin, Literal(float(conf_min), datatype=XSD.double)))
            if pd.notna(conf_mean):
                g.add((obs_occ, TRAFFIC.confidenceMean, Literal(float(conf_mean), datatype=XSD.double)))
            if pd.notna(cov_o):
                g.add((obs_occ, TRAFFIC.coverageOccupancy, Literal(float(cov_o), datatype=XSD.double)))
            if pd.notna(imputed_rate):
                g.add((obs_occ, TRAFFIC.imputedRate, Literal(float(imputed_rate), datatype=XSD.double)))
            if pd.notna(obs_rate):
                g.add((obs_occ, TRAFFIC.cleanObservedRate, Literal(float(obs_rate), datatype=XSD.double)))

            total_obs += 1

print("Total observations minted (count+occupancy):", total_obs)
g.serialize(destination=OUT_TTL, format="turtle")
print("✅ RDF saved:", OUT_TTL, "triples:", len(g))

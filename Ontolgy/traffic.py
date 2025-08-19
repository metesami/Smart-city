# pip install rdflib pandas

import json, urllib.parse
import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD

# --- Namespaces ---
EX   = Namespace("http://example.org/traffic/")
CTDO = Namespace("https://w3id.org/ctdo#")
SOSA = Namespace("http://www.w3.org/ns/sosa/")
TIME = Namespace("http://www.w3.org/2006/time#")

# --- Load URI maps from the SAME paths the metadata script wrote ---
SENSOR_MAP_JSON  = "/content/drive/MyDrive/Smart-city/sensor_uri_map.json"
LANE_MAP_JSON    = "/content/drive/MyDrive/Smart-city/lane_uri_map.json"
SENSOR2LANE_JSON = "/content/drive/MyDrive/Smart-city/sensor_to_lane_map.json"

sensor_uri_map = json.load(open(SENSOR_MAP_JSON))
lane_uri_map   = json.load(open(LANE_MAP_JSON))
try:
    sensor_to_lane_map = json.load(open(SENSOR2LANE_JSON))
except Exception:
    sensor_to_lane_map = {}

print("Loaded maps:",
      "\n  sensors     :", len(sensor_uri_map),
      "\n  lanes       :", len(lane_uri_map),
      "\n  sensor→lane :", len(sensor_to_lane_map))

# --- RDF graph ---
g = Graph()
g.bind("ex", EX)
g.bind("ctdo", CTDO)
g.bind("sosa", SOSA)
g.bind("time", TIME)

# Observable properties
VehicleCount     = EX.VehicleCount;     g.add((VehicleCount, RDF.type, SOSA.ObservableProperty))
AverageDwellTime = EX.AverageDwellTime; g.add((AverageDwellTime, RDF.type, SOSA.ObservableProperty))

# avoid re-adding the same Instant
time_inst_added = set()

# --- Load traffic CSV in chunks ---
chunk_size = 400
traffic_chunks = pd.read_csv(
    "/content/drive/MyDrive/Test ontology_A142/1 day A142 text.csv",
    chunksize=chunk_size
)

triples_to_add = []

for chunk in traffic_chunks:
    timestamps       = chunk["Intervallbeginn (UTC)"]
    intersection_ids = chunk["Anlage"].astype(str).str.strip()
    count_cols       = [c for c in chunk.columns if "(Belegungen/Intervall)" in c]

    for ccol in count_cols:
        sid = ccol.split(" ")[0].strip()
        sensor_uri_str = sensor_uri_map.get(sid)
        if not sensor_uri_str:
            # unknown sensor id — skip
            continue
        sensor_uri = URIRef(sensor_uri_str)

        dwell_col   = f"{sid} (Verweilzeit/Intervall) [ms]"
        has_dwell   = dwell_col in chunk.columns
        count_series = chunk[ccol]
        dwell_series = chunk[dwell_col] if has_dwell else None

        for i in range(len(chunk)):
            count_val = count_series.iloc[i]

            # Parse timestamp (source column is UTC)
            t_raw = str(timestamps.iloc[i])
            try:
                # explicit UTC, timezone-aware
                t_dt = pd.to_datetime(t_raw, format="%d.%m.%Y %H:%M:%S", utc=True)
            except Exception:
                try:
                    t_dt = pd.to_datetime(t_raw, utc=True)  # fallback parser
                except Exception:
                    continue

            iso_t = t_dt.isoformat()  # e.g., "2022-02-01T00:00:00+00:00"


            # time:Instant node
            t_key  = urllib.parse.quote_plus(iso_t)
            t_inst = EX[f"t_{t_key}"]
            if t_key not in time_inst_added:
                triples_to_add.append((t_inst, RDF.type, TIME.Instant))
                triples_to_add.append((t_inst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
                time_inst_added.add(t_key)

            # intersection
            inter_id         = intersection_ids.iloc[i]
            intersection_uri = EX[f"intersection_{inter_id}"]

            # lane via sensor→lane map (if available)
            lane_uri_str = sensor_to_lane_map.get(sid)
            lane_uri     = URIRef(lane_uri_str) if lane_uri_str else None

            # Vehicle Count Observation
            obs_count = EX[f"obsCount_{sid}_{t_key}"]
            triples_to_add.append((obs_count, RDF.type, SOSA.Observation))
            triples_to_add.append((obs_count, SOSA.madeBySensor, sensor_uri))
            triples_to_add.append((obs_count, SOSA.observedProperty, VehicleCount))
            triples_to_add.append((obs_count, SOSA.hasSimpleResult, Literal(int(count_val), datatype=XSD.integer)))
            triples_to_add.append((obs_count, SOSA.phenomenonTime, t_inst))
            triples_to_add.append((obs_count, CTDO.belongsToIntersection, intersection_uri))
            if lane_uri:
                triples_to_add.append((obs_count, SOSA.hasFeatureOfInterest, lane_uri))

            # Average Dwell Time Observation (only if column exists and value present)
            if has_dwell:
                dwell_val = dwell_series.iloc[i]
                if pd.notna(dwell_val):
                    obs_dwell = EX[f"obsDwell_{sid}_{t_key}"]
                    triples_to_add.append((obs_dwell, RDF.type, SOSA.Observation))
                    triples_to_add.append((obs_dwell, SOSA.madeBySensor, sensor_uri))
                    triples_to_add.append((obs_dwell, SOSA.observedProperty, AverageDwellTime))
                    triples_to_add.append((obs_dwell, SOSA.hasSimpleResult, Literal(float(dwell_val), datatype=XSD.double)))
                    triples_to_add.append((obs_dwell, SOSA.phenomenonTime, t_inst))
                    triples_to_add.append((obs_dwell, CTDO.belongsToIntersection, intersection_uri))
                    if lane_uri:
                        triples_to_add.append((obs_dwell, SOSA.hasFeatureOfInterest, lane_uri))

    # flush this chunk
    for s, p, o in triples_to_add:
        g.add((s, p, o))
    triples_to_add.clear()

# --- Save ---
output_path = "/content/drive/MyDrive/Smart-city/A142_traffic_with_intersection.ttl"
g.serialize(destination=output_path, format="turtle")
print(f"✔️ RDF saved with intersection links: {len(g)} triples")

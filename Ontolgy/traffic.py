# pip install rdflib pandas

import json
import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD
import urllib.parse

# ---- Namespaces ----
EX   = Namespace("http://example.org/traffic/")
CTDO = Namespace("https://w3id.org/ctdo#")
SOSA = Namespace("http://www.w3.org/ns/sosa/")
TIME = Namespace("http://www.w3.org/2006/time#")

# ---- Load URI maps created by the metadata script ----
sensor_uri_map = json.load(open("/content/sensor_uri_map.json"))
lane_uri_map   = json.load(open("/content/lane_uri_map.json"))


try:
    sensor_to_lane_map = json.load(open("/content/sensor_to_lane_map.json"))
except Exception:
    sensor_to_lane_map = {}

# ---- RDF graph ----
g = Graph()
g.bind("ex", EX)
g.bind("ctdo", CTDO)
g.bind("sosa", SOSA)
g.bind("time", TIME)

# Declare observable properties once
VehicleCount       = EX.VehicleCount
AverageDwellTime   = EX.AverageDwellTime
g.add((VehicleCount, RDF.type, SOSA.ObservableProperty))
g.add((AverageDwellTime, RDF.type, SOSA.ObservableProperty))

# ---- Load traffic CSV in chunks ----
chunk_size = 400
traffic_chunks = pd.read_csv(
    "/content/drive/MyDrive/Test ontology_A142/1 day A142 text.csv",
    chunksize=chunk_size
)

triples_to_add = []

for chunk in traffic_chunks:
    timestamps = chunk["Intervallbeginn (UTC)"]
    intersection_ids = chunk["Anlage"].astype(str).str.strip()
    count_cols = [c for c in chunk.columns if "(Belegungen/Intervall)" in c]

    for ccol in count_cols:
        sid = ccol.split(" ")[0].strip()
        sensor_uri_str = sensor_uri_map.get(sid)
        if not sensor_uri_str:
            continue
        sensor_uri = URIRef(sensor_uri_str)

        dwell_col = f"{sid} (Verweilzeit/Intervall) [ms]"
        has_dwell = dwell_col in chunk.columns
        count_series = chunk[ccol]
        dwell_series = chunk[dwell_col] if has_dwell else None

        for i in range(len(chunk)):
            count_val = count_series.iloc[i]
            dwell_val = dwell_series.iloc[i]

            # Parse timestamp
            t_raw = str(timestamps.iloc[i])
            try:
                t_dt = pd.to_datetime(t_raw, format="%d.%m.%Y %H:%M:%S")
            except Exception:
                try:
                    t_dt = pd.to_datetime(t_raw)
                except Exception:
                    continue
            iso_t = t_dt.isoformat()

            # time:Instant node
            t_key = urllib.parse.quote_plus(iso_t)
            t_inst = EX[f"t_{t_key}"]
            triples_to_add.append((t_inst, RDF.type, TIME.Instant))
            triples_to_add.append((t_inst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))

            # intersection
            inter_id = intersection_ids.iloc[i]
            intersection_uri = EX[f"intersection_{inter_id}"]

            # lane
            lane_uri_str = sensor_to_lane_map.get(sid)
            lane_uri = URIRef(lane_uri_str) if lane_uri_str else None

            # === Vehicle Count Observation ===
            obs_count = EX[f"obsCount_{sid}_{t_key}"]
            triples_to_add.append((obs_count, RDF.type, SOSA.Observation))
            triples_to_add.append((obs_count, SOSA.madeBySensor, sensor_uri))
            triples_to_add.append((obs_count, SOSA.observedProperty, VehicleCount))
            triples_to_add.append((obs_count, SOSA.hasSimpleResult, Literal(int(count_val), datatype=XSD.integer)))
            triples_to_add.append((obs_count, SOSA.phenomenonTime, t_inst))
            triples_to_add.append((obs_count, CTDO.belongsToIntersection, intersection_uri))
            if lane_uri:
                triples_to_add.append((obs_count, SOSA.hasFeatureOfInterest, lane_uri))

            # === Average Dwell Time Observation ===
            obs_dwell = EX[f"obsDwell_{sid}_{t_key}"]
            g.add((obs_dwell, RDF.type, SOSA.Observation))
            g.add((obs_dwell, SOSA.madeBySensor, sensor_uri))
            g.add((obs_dwell, SOSA.observedProperty, AverageDwellTime))
            g.add((obs_dwell, SOSA.hasSimpleResult, Literal(float(dwell_val), datatype=XSD.double)))
            g.add((obs_dwell, SOSA.phenomenonTime, t_inst))
            g.add((obs_dwell, CTDO.belongsToIntersection, intersection_uri))
            if lane_uri:
                g.add((obs_dwell, SOSA.hasFeatureOfInterest, lane_uri))

    # Add all triples for this chunk
    for s, p, o in triples_to_add:
        g.add((s, p, o))
    triples_to_add.clear()

# ---- Save ----
output_path = "/content/A142_traffic_with_intersection.ttl"
g.serialize(destination=output_path, format="turtle")
print(f"✔️ RDF saved with intersection links: {len(g)} triples")

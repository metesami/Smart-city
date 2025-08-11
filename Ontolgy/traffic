pip install rdflib
import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
import urllib.parse

#  Namespaces
EX = Namespace("http://example.org/traffic/")
CTDO = Namespace("https://w3id.org/ctdo#")
SOSA = Namespace("http://www.w3.org/ns/sosa/")

# Load sensor URI map from intersection ontology
sensor_uri_map = json.load(open("/content/sensor_uri_map.json"))

# Create RDF graph
g = Graph()
g.bind("ex", EX)
g.bind("ctdo", CTDO)
g.bind("sosa", SOSA)

#  Load CSV in chunks
chunk_size = 400
traffic_chunks = pd.read_csv(
    '/content/drive/MyDrive/Test ontology_A142/1 day A142 text.csv',
    chunksize=chunk_size
)

for chunk in traffic_chunks:
    timestamps = chunk["Intervallbeginn (UTC)"]
    occupancy_cols = [col for col in chunk.columns if "(Belegungen/Intervall)" in col]

    triples_to_add = []

    for occ_col in occupancy_cols:
        sid = occ_col.split(" ")[0]
        count_data = chunk[occ_col]
        dwell_col = f"{sid} (Verweilzeit/Intervall) [ms]"
        dwell_data = chunk.get(dwell_col, None)

        for i in range(len(chunk)):
            timestamp_str = str(timestamps.iloc[i])
            count = count_data.iloc[i]
            dwell_time = dwell_data.iloc[i] if dwell_data is not None else None

            if pd.isna(count):
                continue

            try:
                timestamp_dt = pd.to_datetime(timestamp_str, format='%d.%m.%Y %H:%M:%S')
                iso_timestamp = timestamp_dt.isoformat()
            except ValueError:
                continue

            # Build URIs
            encoded_timestamp = urllib.parse.quote_plus(iso_timestamp)
            obs_uri = EX[f"obs_{sid}_{encoded_timestamp}"]
            sensor_uri = EX[f"sensor_{sid}"]

            # Get intersection ID from 'Anlage'
            intersection_id = str(chunk["Anlage"].iloc[i]).strip()
            intersection_uri = EX[f"intersection_{intersection_id}"]

            # Observation triples
            triples_to_add.append((obs_uri, RDF.type, CTDO.SensorObservation))
            triples_to_add.append((obs_uri, RDF.type, SOSA.Observation))
            triples_to_add.append((obs_uri, SOSA.madeBySensor, sensor_uri))
            triples_to_add.append((obs_uri, SOSA.resultTime, Literal(iso_timestamp, datatype=XSD.dateTime)))
            triples_to_add.append((obs_uri, CTDO.belongsToIntersection, intersection_uri))  # Link observation → intersection


            # Sensor triples (link sensor to intersection)
            triples_to_add.append((sensor_uri, CTDO.belongsToIntersection, intersection_uri))
            triples_to_add.append((obs_uri, CTDO.belongsToIntersection, intersection_uri))

            # Result literal
            result_literal = f"Occupancy: {dwell_time}ms, Count: {count}"
            triples_to_add.append((obs_uri, SOSA.hasResult, Literal(result_literal)))

    for s, p, o in triples_to_add:
        g.add((s, p, o))

#  Save as Turtle
output_path = "/content/A142_traffic_with_intersection.ttl"
g.serialize(destination=output_path, format='turtle')
print(f"✔️ RDF saved with intersection links: {len(g)} triples")

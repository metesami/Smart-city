import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD
import urllib.parse

#  1. Setup RDF Graph and Namespaces 
g = Graph()
EX = Namespace("http://example.org/pollution/")
POLL = Namespace("https://w3id.org/airpollution#")
SOSA = Namespace("http://www.w3.org/ns/sosa/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
g.bind("ex", EX)
g.bind("poll", POLL)
g.bind("sosa", SOSA)
g.bind("geo", GEO)

#  2. Metadata for pollution stations 
# Add encoding parameter to handle potential decoding issues
metadata = pd.read_csv("/content/drive/MyDrive/Test ontology_A142/Copy of pollution_stations_metadata.csv", sep=",", encoding='latin-1')

#  3. Create pollution station nodes 
station_uri_map = {}
for _, row in metadata.iterrows():
    sid = row["StationID"]
    station_uri = EX[f"station_{sid}"]
    station_uri_map[sid] = station_uri

    g.add((station_uri, RDF.type, POLL.PollutionStation))
    g.add((station_uri, POLL.stationId, Literal(sid)))
    g.add((station_uri, POLL.address, Literal(row["address"])))
    g.add((station_uri, GEO.lat, Literal(row["latitude"], datatype=XSD.float)))
    g.add((station_uri, GEO.long, Literal(row["longitude"], datatype=XSD.float)))
    g.add((station_uri, POLL.osmId, Literal(row["OSM_ID"])))

#  4. Load pollution data in chunks 
file_path = "/content/drive/MyDrive/Test ontology_A142/1 day pollution.csv"  # Correct path
chunk_size = 500
# Add encoding parameter to handle potential decoding issues for the pollution data file as well
pollution_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, decimal=',',encoding='latin-1')


for chunk in pollution_chunks:
    triples_to_add = []

    for i in range(len(chunk)):
        row = chunk.iloc[i]
        # Use the correct column name 'ï»¿datetime' based on notebook state
        timestamp_str = str(row["ï»¿datetime"])
        try:
            timestamp_dt = pd.to_datetime(timestamp_str)
            iso_timestamp = timestamp_dt.isoformat()
        except Exception:
            continue

        encoded_timestamp = urllib.parse.quote_plus(iso_timestamp)
        station_id = row["StationID"]
        obs_uri = EX[f"pollution_{station_id}_{encoded_timestamp}"]
        station_uri = station_uri_map.get(station_id)

        triples_to_add.append((obs_uri, RDF.type, POLL.PollutionObservation))
        triples_to_add.append((obs_uri, SOSA.resultTime, Literal(iso_timestamp, datatype=XSD.dateTime)))
        if station_uri:
            triples_to_add.append((obs_uri, SOSA.madeBySensor, station_uri))

        # Add pollution data if available
        if not pd.isna(row.get("NO2")):
            triples_to_add.append((obs_uri, POLL.NO2, Literal(float(row["NO2"]))))
        if not pd.isna(row.get("PM10")):
            triples_to_add.append((obs_uri, POLL.PM10, Literal(float(row["PM10"]))))
        if not pd.isna(row.get("PM2.5")):
            triples_to_add.append((obs_uri, POLL.PM2_5, Literal(float(row["PM2.5"]))))

    for s, p, o in triples_to_add:
        g.add((s, p, o))

#  5. Save output 
output_path = "/content/A142_pollution_ontology.ttl"
g.serialize(destination=output_path, format="turtle")
print(f"✅ Done! Total triples in pollution ontology: {len(g)}")
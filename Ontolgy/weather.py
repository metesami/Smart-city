import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, XSD
import urllib.parse

# === 1. Setup RDF Graph and Namespaces ===
g = Graph()
EX = Namespace("http://example.org/weather/")
WMO = Namespace("https://wmo.org/ontology#")
SOSA = Namespace("http://www.w3.org/ns/sosa/")
g.bind("ex", EX)
g.bind("wmo", WMO)
g.bind("sosa", SOSA)

# === 2. Define File Path and Chunksize ===
file_path = '/content/drive/MyDrive/Test ontology_A142/1 day weather.csv'
chunk_size = 1000  # Adjust based on size and memory

# === 3. Process Weather Data in Chunks ===
weather_chunks = pd.read_csv(file_path, sep=";", chunksize=chunk_size)

for chunk in weather_chunks:

    triples_to_add = []

    for i in range(len(chunk)):
        row = chunk.iloc[i]

        timestamp_str = str(row["datetime"])
        try:
            timestamp_dt = pd.to_datetime(timestamp_str)
            iso_timestamp = timestamp_dt.isoformat()
        except Exception:
            continue  # skip if timestamp is invalid

        encoded_timestamp = urllib.parse.quote_plus(iso_timestamp)
        weather_uri = EX[f"weather_{encoded_timestamp}"]

        # Add base triples
        triples_to_add.append((weather_uri, RDF.type, WMO.WeatherObservation))
        triples_to_add.append((weather_uri, SOSA.resultTime, Literal(iso_timestamp, datatype=XSD.dateTime)))

        # Add weather properties (skip NaNs)
        if not pd.isna(row.get("TT_10")):
            triples_to_add.append((weather_uri, WMO.temperature, Literal(float(row["TT_10"]))))
        if not pd.isna(row.get("RF_10")):
            triples_to_add.append((weather_uri, WMO.humidity, Literal(float(row["RF_10"]))))
        if not pd.isna(row.get("PP_10")):
            triples_to_add.append((weather_uri, WMO.pressure, Literal(float(row["PP_10"]))))
        if not pd.isna(row.get("RWS_10")):
            triples_to_add.append((weather_uri, WMO.precipitation, Literal(float(row["RWS_10"]))))
        if not pd.isna(row.get("RWS_IND_10")):
            triples_to_add.append((weather_uri, WMO.rainIndicator, Literal(int(row["RWS_IND_10"]))))
        if not pd.isna(row.get("wind_speed")):
            triples_to_add.append((weather_uri, WMO.windSpeed, Literal(float(row["wind_speed"]))))
        if not pd.isna(row.get("wind_direction")):
            triples_to_add.append((weather_uri, WMO.windDirection, Literal(float(row["wind_direction"]))))

    # === 4. Add all triples from this chunk to graph ===
    for s, p, o in triples_to_add:
        g.add((s, p, o))

# === 5. Save to Turtle File ===
g.serialize(destination="/content/A142_weather_ontology.ttl", format="turtle")
print(f"✅ Done! Total triples in weather ontology: {len(g)}")

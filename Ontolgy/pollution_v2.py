# pip install rdflib pandas

import pandas as pd, urllib.parse
from decimal import Decimal
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD
import datetime as dt

g = Graph()
EX        = Namespace("http://example.org/pollution/")
SC        = Namespace("http://example.org/smartcity/core#")
POLLUTION = Namespace("http://example.org/smartcity/pollution#")
SOSA      = Namespace("http://www.w3.org/ns/sosa/")
TIME      = Namespace("http://www.w3.org/2006/time#")
GEO       = Namespace("http://www.opengis.net/ont/geosparql#")
QUDT      = Namespace("http://qudt.org/schema/qudt/")
UNIT      = Namespace("http://qudt.org/vocab/unit/")

g.bind("ex", EX)
g.bind("sc", SC)
g.bind("pollution", POLLUTION)
g.bind("sosa", SOSA)
g.bind("time", TIME)
g.bind("geo", GEO)
g.bind("qudt", QUDT)
g.bind("unit", UNIT)

metadata_path = "/content/drive/MyDrive/Test ontology_A142/Pollution/pollution_stations_metadata.csv"
metadata = pd.read_csv(metadata_path, sep=",", encoding="latin-1")

station_uri_map = {}
sensor_uri_map  = {}

for _, row in metadata.iterrows():
    sid      = str(row["StationID"]).strip()
    platform = EX[f"station_{sid}"]
    sens     = EX[f"sensor_{sid}"]

    station_uri_map[sid] = str(platform)
    sensor_uri_map[sid]  = str(sens)

    g.add((platform, RDF.type, POLLUTION.PollutionPlatform))
    g.add((platform, RDFS.label, Literal(str(row.get("address","")).strip() or f"Station {sid}")))

    if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
        lat = Decimal(str(row["latitude"]))
        lon = Decimal(str(row["longitude"]))

        geom = EX[f"geom_station_{sid}"]
        g.add((platform, GEO.hasGeometry, geom))
        g.add((geom, RDF.type, GEO.Geometry))
        g.add((geom, GEO.asWKT, Literal(f"POINT({lon} {lat})", datatype=GEO.wktLiteral)))

        g.add((platform, POLLUTION.stationLatitude,  Literal(lat, datatype=XSD.decimal)))
        g.add((platform, POLLUTION.stationLongitude, Literal(lon, datatype=XSD.decimal)))

    g.add((platform, POLLUTION.stationId, Literal(sid)))

    try:
        osm_id = int(row["OSM_ID"])
        g.add((platform, POLLUTION.osmNodeId, Literal(int(osm_id), datatype=XSD.long)))
    except Exception:
        pass

    g.add((sens, RDF.type, POLLUTION.PollutionSensor))
    g.add((sens, SOSA.isHostedBy, platform))
    g.add((platform, SOSA.hosts, sens))

time_inst_added = set()

def epoch_to_iso(ts_seconds: int) -> str:
    return dt.datetime.fromtimestamp(int(ts_seconds), tz=dt.timezone.utc).isoformat()

file_path  = "/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/pollution_10min_by_station.csv"
chunk_size = 500
pollution_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, encoding="utf-8", low_memory=False)

def ensure_timestamp_seconds(row):
    ts = row.get("timestamp_seconds")
    try:
        if pd.notna(ts):
            return int(ts)
    except Exception:
        pass
    dts = row.get("datetime")
    if pd.isna(dts) or dts is None:
        return None
    try:
        return int(pd.Timestamp(dts).timestamp())
    except Exception:
        return None

for chunk in pollution_chunks:
    if "ï»¿datetime" in chunk.columns and "datetime" not in chunk.columns:
        chunk = chunk.rename(columns={"ï»¿datetime": "datetime"})
    if "ï»¿StationID" in chunk.columns and "StationID" not in chunk.columns:
        chunk = chunk.rename(columns={"ï»¿StationID": "StationID"})

    triples = []

    for _, row in chunk.iterrows():
        sid = str(row.get("StationID","")).strip()
        if not sid:
            continue

        platform_uri = station_uri_map.get(sid)
        sens_uri     = sensor_uri_map.get(sid)
        if not platform_uri or not sens_uri:
            continue

        platform = URIRef(platform_uri)
        sens     = URIRef(sens_uri)

        timestamp_seconds = ensure_timestamp_seconds(row)
        if timestamp_seconds is None:
            continue

        iso_t = epoch_to_iso(timestamp_seconds)
        t_key = urllib.parse.quote_plus(iso_t)
        tinst = EX[f"t_{t_key}"]
        if t_key not in time_inst_added:
            triples.append((tinst, RDF.type, TIME.Instant))
            triples.append((tinst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(t_key)

        def add_obs(prop_uri, val, unit_uri=None):
            oname = str(prop_uri).split("#")[-1].split("/")[-1]
            obs   = EX[f"obs_{sid}_{timestamp_seconds}_{oname}"]

            triples.extend([
                (obs, RDF.type, POLLUTION.PollutionObservation),
                (obs, RDF.type, SOSA.Observation),

                (obs, SOSA.madeBySensor, sens),
                (sens, SOSA.madeObservation, obs),

                (obs, SOSA.observedProperty, prop_uri),
                (sens, SOSA.observes, prop_uri),
                (prop_uri, SOSA.isObservedBy, sens),

                (obs, SOSA.hasSimpleResult, Literal(float(val), datatype=XSD.double)),
                (obs, SOSA.phenomenonTime, tinst),
                (obs, SOSA.hasFeatureOfInterest, platform),
                (obs, SC.observedAtTimeIndex, Literal(int(timestamp_seconds), datatype=XSD.long)),
            ])

            if unit_uri:
                triples.append((obs, QUDT.unit, unit_uri))

        if pd.notna(row.get("NO2")):
            add_obs(POLLUTION.NO2, row["NO2"], UNIT["MicroGM-PER-M3"])

        if pd.notna(row.get("PM10")):
            add_obs(POLLUTION.PM10, row["PM10"], UNIT["MicroGM-PER-M3"])

        if pd.notna(row.get("PM2.5")):
            add_obs(POLLUTION.PM25, row["PM2.5"], UNIT["MicroGM-PER-M3"])

    for t in triples:
        g.add(t)

output_path = "/content/drive/MyDrive/Smart-city/A142_pollution_ontology.ttl"
g.serialize(destination=output_path, format="turtle")
print(f"✅ Done! Total triples in pollution ontology: {len(g)}")
print("Saved to:", output_path)

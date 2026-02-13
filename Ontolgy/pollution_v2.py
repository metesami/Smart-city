# pip install rdflib pandas

import pandas as pd, urllib.parse
from decimal import Decimal
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD
import datetime as dt

# Setup RDF Graph and Namespaces
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

# Metadata for pollution stations
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
    g.add((platform, RDFS.label, Literal(row.get("address","").strip() or f"Station {sid}")))

    if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
        lat = Decimal(row["latitude"])
        lon = Decimal(row["longitude"])

        geom = EX[f"geom_station_{sid}"]
        g.add((platform, GEO.hasGeometry, geom))
        g.add((geom, RDF.type, GEO.Geometry))
        g.add((geom, GEO.asWKT, Literal(f"POINT({lon} {lat})", datatype=GEO.wktLiteral)))

        g.add((platform, POLLUTION.stationLatitude,  Literal(lat, datatype=XSD.decimal)))
        g.add((platform, POLLUTION.stationLongitude, Literal(lon, datatype=XSD.decimal)))

    g.add((platform, POLLUTION.stationId, Literal(sid)))
    osm_id = int(row["OSM_ID"])
    g.add((platform, POLLUTION.osmNodeId, Literal(osm_id, datatype=XSD.string)))

    g.add((sens, RDF.type, POLLUTION.PollutionSensor))
    g.add((sens, SOSA.isHostedBy, platform))
    g.add((platform, SOSA.hosts, sens))

# Time cache
time_inst_added = set()

# Load pollution data in chunks
file_path  = "/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/pollution_10min.csv"
chunk_size = 500
pollution_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, decimal=",", encoding="latin-1")

only_stationID = "DEHE040"
pollution_bins = set()

def epoch_to_iso(ts_seconds: int) -> str:
    # timestamp_seconds is UTC epoch seconds
    return dt.datetime.fromtimestamp(ts_seconds, tz=dt.timezone.utc).isoformat()

for chunk in pollution_chunks:
    if "ï»¿datetime" in chunk.columns and "datetime" not in chunk.columns:
        chunk = chunk.rename(columns={"ï»¿datetime": "datetime"})

    triples = []

    for _, row in chunk.iterrows():
        sid = str(only_stationID)
        platform_uri = station_uri_map.get(sid)
        sens_uri     = sensor_uri_map.get(sid)
        if not platform_uri or not sens_uri:
            continue

        platform = URIRef(platform_uri)
        sens     = URIRef(sens_uri)

        ts_seconds_val = row.get("timestamp_seconds")
        try:
            timestamp_seconds = int(ts_seconds_val) if pd.notna(ts_seconds_val) else None
        except Exception:
            timestamp_seconds = None

        if timestamp_seconds is None:
            continue

        # Build time:Instant from epoch seconds
        iso_t = epoch_to_iso(timestamp_seconds)
        t_key = urllib.parse.quote_plus(iso_t)
        tinst = EX[f"t_{t_key}"]
        if t_key not in time_inst_added:
            triples.append((tinst, RDF.type, TIME.Instant))
            triples.append((tinst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(t_key)

        def add_obs(prop_uri, val, unit_uri=None, category_label=None):
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

                # ✅ for prediction/causal
                (obs, SOSA.hasSimpleResult, Literal(float(val), datatype=XSD.double)),

                # ✅ semantic time
                (obs, SOSA.phenomenonTime, tinst),

                # FOI (your current design uses platform/station as FOI)
                (obs, SOSA.hasFeatureOfInterest, platform),

                # ✅ for TKGE/ATiSE
                (obs, SC.observedAtTimeIndex, Literal(timestamp_seconds, datatype=XSD.long)),
            ])

            if unit_uri:
                triples.append((obs, QUDT.unit, unit_uri))

            if category_label and pd.notna(category_label):
                cat_label = str(category_label).strip()
                if cat_label:
                    cat_uri = URIRef(str(POLLUTION) + cat_label)
                    triples.append((obs, POLLUTION.hasCategory, cat_uri))
                    triples.append((cat_uri, POLLUTION.isCategoryOf, obs))

                    sub_category_uri = POLLUTION[f"{oname}_Category"]
                    if cat_uri not in pollution_bins:
                        triples.append((cat_uri, RDF.type, sub_category_uri))
                        triples.append((sub_category_uri, RDF.type, POLLUTION.PollutionCategory))
                        triples.append((cat_uri, RDFS.label, Literal(cat_label)))
                        pollution_bins.add(cat_uri)

        if pd.notna(row.get("NO2")):
            add_obs(POLLUTION.NO2, row["NO2"], UNIT["MicroGM-PER-M3"], category_label=row.get("NO2_category"))

        if pd.notna(row.get("PM10")):
            add_obs(POLLUTION.PM10, row["PM10"], UNIT["MicroGM-PER-M3"], category_label=row.get("PM10_category"))

        if pd.notna(row.get("PM2.5")):
            add_obs(POLLUTION.PM25, row["PM2.5"], UNIT["MicroGM-PER-M3"], category_label=row.get("PM2.5_category"))

    for t in triples:
        g.add(t)

output_path = "/content/drive/MyDrive/Smart-city/A142_pollution_ontology.ttl"
g.serialize(destination=output_path, format="turtle")
print(f"✅ Done! Total triples in pollution ontology: {len(g)}")

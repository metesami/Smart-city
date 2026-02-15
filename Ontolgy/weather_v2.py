# pip install rdflib pandas

import pandas as pd, urllib.parse
from decimal import Decimal
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD
import datetime as dt

# ---------------- RDF Graph + Namespaces ----------------
g = Graph()
EX      = Namespace("http://example.org/weather/")
SC      = Namespace("http://example.org/smartcity/core#")
WEATHER = Namespace("http://example.org/smartcity/weather#")
SOSA    = Namespace("http://www.w3.org/ns/sosa/")
TIME    = Namespace("http://www.w3.org/2006/time#")
GEO     = Namespace("http://www.opengis.net/ont/geosparql#")
QUDT    = Namespace("http://qudt.org/schema/qudt/")
UNIT    = Namespace("http://qudt.org/vocab/unit/")

g.bind("ex", EX)
g.bind("sc", SC)
g.bind("weather", WEATHER)
g.bind("sosa", SOSA)
g.bind("time", TIME)
g.bind("geo", GEO)
g.bind("qudt", QUDT)
g.bind("unit", UNIT)

# ---------------- Metadata (like pollution) ----------------
metadata_path = "/mnt/data/weather_stations_metadata.csv"
metadata = pd.read_csv(metadata_path, sep=",", encoding="utf-8", low_memory=False)

station_uri_map = {}
sensor_uri_map  = {}

for _, row in metadata.iterrows():
    sid = str(row.get("StationID", "")).strip()
    if not sid:
        continue

    platform = EX[f"station_{sid}"]
    sens     = EX[f"sensor_{sid}"]

    station_uri_map[sid] = str(platform)
    sensor_uri_map[sid]  = str(sens)

    g.add((platform, RDF.type, WEATHER.WeatherPlatform))
    g.add((platform, RDFS.label, Literal(str(row.get("address","")).strip() or f"Weather Station {sid}")))

    # lat/lon if present
    lat = row.get("latitude", None)
    lon = row.get("longitude", None)
    if pd.notna(lat) and pd.notna(lon):
        lat_d = Decimal(str(lat))
        lon_d = Decimal(str(lon))

        geom = EX[f"geom_station_{sid}"]
        g.add((platform, GEO.hasGeometry, geom))
        g.add((geom, RDF.type, GEO.Geometry))
        g.add((geom, GEO.asWKT, Literal(f"POINT({lon_d} {lat_d})", datatype=GEO.wktLiteral)))

        # optional explicit properties (if your ontology has them)
        g.add((platform, WEATHER.stationLatitude,  Literal(lat_d, datatype=XSD.decimal)))
        g.add((platform, WEATHER.stationLongitude, Literal(lon_d, datatype=XSD.decimal)))

    g.add((platform, WEATHER.stationId, Literal(sid)))

    # optional OSM id
    if "OSM_ID" in metadata.columns:
        try:
            osm_id = int(row["OSM_ID"])
            g.add((platform, WEATHER.osmNodeId, Literal(int(osm_id), datatype=XSD.long)))
        except Exception:
            pass

    g.add((sens, RDF.type, WEATHER.WeatherSensor))
    g.add((sens, SOSA.isHostedBy, platform))
    g.add((platform, SOSA.hosts, sens))

# ---------------- Helpers ----------------
time_inst_added = set()

def epoch_to_iso(ts_seconds: int) -> str:
    return dt.datetime.fromtimestamp(int(ts_seconds), tz=dt.timezone.utc).isoformat()

def ensure_timestamp_seconds(row):
    ts = row.get("timestamp_seconds", None)
    try:
        if pd.notna(ts):
            return int(ts)
    except Exception:
        pass

    dts = row.get("datetime", None)
    if pd.isna(dts) or dts is None:
        return None
    try:
        return int(pd.Timestamp(dts).timestamp())
    except Exception:
        return None

def add_obs(triples, sid, platform, sens, tinst, t_idx, prop_uri, val, unit_uri=None, xsd_dt=XSD.double):
    oname = str(prop_uri).split("#")[-1].split("/")[-1]
    obs   = EX[f"obs_{sid}_{t_idx}_{oname}"]

    triples.extend([
        (obs, RDF.type, WEATHER.WeatherObservation),
        (obs, RDF.type, SOSA.Observation),

        (obs, SOSA.madeBySensor, sens),
        (sens, SOSA.madeObservation, obs),

        (obs, SOSA.observedProperty, prop_uri),
        (sens, SOSA.observes, prop_uri),
        (prop_uri, SOSA.isObservedBy, sens),

        (obs, SOSA.hasSimpleResult, Literal(val, datatype=xsd_dt)),
        (obs, SOSA.phenomenonTime, tinst),
        (obs, SOSA.hasFeatureOfInterest, platform),

        (obs, SC.observedAtTimeIndex, Literal(int(t_idx), datatype=XSD.long)),
    ])

    if unit_uri:
        triples.append((obs, QUDT.unit, unit_uri))

# ---------------- Load weather 10-min CSV ----------------
file_path  = "/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/weather_10min_by_station.csv"
chunk_size = 1000
weather_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, encoding="utf-8", low_memory=False)

for chunk in weather_chunks:
    # BOM fixes
    if "ï»¿datetime" in chunk.columns and "datetime" not in chunk.columns:
        chunk = chunk.rename(columns={"ï»¿datetime": "datetime"})
    if "ï»¿StationID" in chunk.columns and "StationID" not in chunk.columns:
        chunk = chunk.rename(columns={"ï»¿StationID": "StationID"})

    triples = []

    for _, row in chunk.iterrows():
        sid = str(row.get("StationID", "")).strip()
        if not sid:
            continue

        platform_uri = station_uri_map.get(sid)
        sens_uri     = sensor_uri_map.get(sid)
        if not platform_uri or not sens_uri:
            continue

        platform = URIRef(platform_uri)
        sens     = URIRef(sens_uri)

        t_idx = ensure_timestamp_seconds(row)
        if t_idx is None:
            continue

        iso_t = epoch_to_iso(t_idx)
        tkey  = urllib.parse.quote_plus(iso_t)
        tinst = EX[f"t_{tkey}"]

        if tkey not in time_inst_added:
            triples.append((tinst, RDF.type, TIME.Instant))
            triples.append((tinst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(tkey)

        # -------- RAW observations (NO bins) --------
        if pd.notna(row.get("temperature")):
            add_obs(triples, sid, platform, sens, tinst, t_idx,
                    WEATHER.Temperature, float(row["temperature"]), UNIT.DEG_C, XSD.double)

        if pd.notna(row.get("humidity")):
            add_obs(triples, sid, platform, sens, tinst, t_idx,
                    WEATHER.RelativeHumidity, float(row["humidity"]), UNIT.PERCENT, XSD.double)

        if pd.notna(row.get("pressure")):
            add_obs(triples, sid, platform, sens, tinst, t_idx,
                    WEATHER.AirPressure, float(row["pressure"]), UNIT.HectoPA, XSD.double)

        if pd.notna(row.get("precipitation")):
            add_obs(triples, sid, platform, sens, tinst, t_idx,
                    WEATHER.Precipitation, float(row["precipitation"]), UNIT.MilliM, XSD.double)

        if pd.notna(row.get("wind_speed")):
            add_obs(triples, sid, platform, sens, tinst, t_idx,
                    WEATHER.WindSpeed, float(row["wind_speed"]), UNIT["M-PER-SEC"], XSD.double)

        if pd.notna(row.get("wind_direction")):
            add_obs(triples, sid, platform, sens, tinst, t_idx,
                    WEATHER.WindDirection, float(row["wind_direction"]), UNIT.DEG, XSD.double)

        if "rain_flag" in chunk.columns and pd.notna(row.get("rain_flag")):
            try:
                add_obs(triples, sid, platform, sens, tinst, t_idx,
                        WEATHER.RainFlag, bool(int(row["rain_flag"])), None, XSD.boolean)
            except Exception:
                pass

    for t in triples:
        g.add(t)

# ---------------- Serialize ----------------
out_path = "/content/drive/MyDrive/Smart-city/A142_weather_ontology.ttl"
g.serialize(destination=out_path, format="turtle")
print(f"✅ Done! Total triples in weather ontology: {len(g)}")
print("Saved to:", out_path)

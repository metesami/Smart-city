# pip install rdflib pandas
"""
Updated pollution raw ABox builder (v2.2)

Design:
- raw pollution observations remain station-specific source truth
- sosa:hasFeatureOfInterest stays the platform/station
- station metadata includes station type
- no derived context triples are mixed into this raw ABox
"""

import pandas as pd
import datetime as dt
from decimal import Decimal
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

# ---------------- RDF Graph + Namespaces ----------------
g = Graph()

EX        = Namespace("http://example.org/pollution/")
SC        = Namespace("http://example.org/smartcity/core#")
POLLUTION = Namespace("http://example.org/smartcity/pollution#")
SOSA      = Namespace("http://www.w3.org/ns/sosa/")
TIME      = Namespace("http://www.w3.org/2006/time#")
GEO       = Namespace("http://www.opengis.net/ont/geosparql#")
QUDT      = Namespace("http://qudt.org/schema/qudt/")
UNIT      = Namespace("http://qudt.org/vocab/unit/")
SCTIME    = Namespace("http://example.org/smartcity/time/")
EXCORE    = Namespace("http://example.org/core/")

g.bind("excore", EXCORE)
g.bind("ex", EX)
g.bind("sc", SC)
g.bind("pollution", POLLUTION)
g.bind("sosa", SOSA)
g.bind("time", TIME)
g.bind("geo", GEO)
g.bind("qudt", QUDT)
g.bind("unit", UNIT)
g.bind("sctime", SCTIME)

# ---------------- Paths ----------------
metadata_path = "/run/determined/workdir/pollution/pollution_stations_metadata.csv"
file_path     = "/run/determined/workdir/pollution/pollution_10min_by_station.csv"
out_path      = "/run/determined/workdir/pollution/pollution_abox_v2.2.ttl"

# ---------------- Helpers ----------------
def dec(v):
    return Decimal(str(v).strip().replace(",", "."))

def epoch_to_iso(ts_seconds: int) -> str:
    return dt.datetime.fromtimestamp(int(ts_seconds), tz=dt.timezone.utc).isoformat()

def ensure_timestamp_seconds(row):
    ts = row.get("timestamp_seconds")
    try:
        if pd.notna(ts):
            return int(float(ts))
    except Exception:
        pass

    dts = row.get("datetime")
    if pd.isna(dts) or dts is None:
        return None

    try:
        return int(pd.Timestamp(dts, tz="UTC").timestamp())
    except Exception:
        return None

def normalize_station_type(value: str):
    v = str(value).strip()
    key = v.lower().replace("-", "").replace("_", "").replace(" ", "")

    if key in {"backgroundlike", "urbanbackgroundlike", "background", "backgroundstation"}:
        return POLLUTION.urbanBackgroundLike

    if key in {"trafficinfluenced", "traffic", "trafficsite", "trafficinfluencedsite", "roadside"}:
        return POLLUTION.trafficInfluenced

    return None

def add_obs(triples, sid, platform, sens, tinst, t_idx, prop_uri, val, unit_uri=None):
    oname = str(prop_uri).split("#")[-1].split("/")[-1]
    obs   = EX[f"obs_{sid}_{t_idx}_{oname}"]

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

        (obs, SC.observedAtTimeIndex, Literal(int(t_idx), datatype=XSD.long)),
        (obs, SC.aggregationWindowSeconds, Literal(600, datatype=XSD.integer)),
    ])

    if unit_uri is not None:
        triples.append((obs, QUDT.unit, unit_uri))

# ---------------- Metadata -> Platforms/Sensors ----------------
metadata = pd.read_csv(metadata_path, sep=",", encoding="latin-1", low_memory=False)

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

    g.add((platform, RDF.type, POLLUTION.PollutionPlatform))
    g.add((platform, SC.locatedIn, EXCORE["darmstadt"]))
    g.add((platform, RDFS.label, Literal(str(row.get("address", "")).strip() or f"Station {sid}")))
    g.add((platform, POLLUTION.stationId, Literal(sid, datatype=XSD.string)))

    address = str(row.get("address", "")).strip()
    if address:
        g.add((platform, POLLUTION.stationAddress, Literal(address, datatype=XSD.string)))

    lat = row.get("latitude")
    lon = row.get("longitude")
    if pd.notna(lat) and pd.notna(lon):
        lat_d = dec(lat)
        lon_d = dec(lon)
        geom = EX[f"station_{sid}_geom_main"]

        g.add((platform, SC.hasGeometry, geom))
        g.add((geom, RDF.type, GEO.Geometry))
        g.add((geom, SC.asWKT, Literal(f"POINT({lon_d} {lat_d})", datatype=GEO.wktLiteral)))

        g.add((platform, POLLUTION.stationLatitude, Literal(lat_d, datatype=XSD.decimal)))
        g.add((platform, POLLUTION.stationLongitude, Literal(lon_d, datatype=XSD.decimal)))

    osm_id = row.get("OSM_ID")
    if pd.notna(osm_id):
        g.add((platform, POLLUTION.osmNodeId, Literal(str(osm_id), datatype=XSD.string)))

    # StationType as object link to a StationType individual
    station_type_uri = normalize_station_type(row.get("StationType"))
    st_raw = str(row.get("StationType", "")).strip()

    if station_type_uri is not None:
        g.add((platform, POLLUTION.hasStationType, station_type_uri))

    if st_raw:
        g.add((platform, POLLUTION.stationTypeLabel, Literal(st_raw, datatype=XSD.string)))

    g.add((sens, RDF.type, POLLUTION.PollutionSensor))
    g.add((sens, SOSA.isHostedBy, platform))
    g.add((platform, SOSA.hosts, sens))

# ---------------- Load pollution 10-min CSV ----------------
time_inst_added = set()
chunk_size = 2000
chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, encoding="utf-8", low_memory=False)

for chunk in chunks:
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

        t_inst = SCTIME[f"t_{t_idx}"]

        if t_idx not in time_inst_added:
            triples.append((t_inst, RDF.type, TIME.Instant))
            triples.append((t_inst, TIME.inXSDDateTime, Literal(epoch_to_iso(t_idx), datatype=XSD.dateTime)))
            time_inst_added.add(t_idx)

        if pd.notna(row.get("NO2")):
            add_obs(triples, sid, platform, sens, t_inst, t_idx, POLLUTION.NO2, row["NO2"], UNIT["MicroGM-PER-M3"])

        if pd.notna(row.get("PM10")):
            add_obs(triples, sid, platform, sens, t_inst, t_idx, POLLUTION.PM10, row["PM10"], UNIT["MicroGM-PER-M3"])

        if pd.notna(row.get("PM2.5")):
            add_obs(triples, sid, platform, sens, t_inst, t_idx, POLLUTION.PM25, row["PM2.5"], UNIT["MicroGM-PER-M3"])

    for t in triples:
        g.add(t)

# ---------------- Serialize ----------------
g.serialize(destination=out_path, format="turtle")
print(f"Done. Total triples: {len(g)}")
print("Saved to:", out_path)
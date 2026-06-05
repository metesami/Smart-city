# pip install rdflib pandas
"""
Weather context ABox builder (separate graph)

Design choice:
- raw weather observations remain station-specific source truth
- this graph contains only the derived city-wide weather context layer
- context is linked to shared targets (city + traffic network)
- provenance is preserved via sc:derivedFromObservation
"""

import pandas as pd
import datetime as dt
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, RDFS, XSD

# ---------------- RDF Graph + Namespaces ----------------
g = Graph()

EXW      = Namespace("http://example.org/weather/")
EXCORE   = Namespace("http://example.org/core/")
EXTRAF   = Namespace("http://example.org/traffic/")

SC       = Namespace("http://example.org/smartcity/core#")
WEATHER  = Namespace("http://example.org/smartcity/weather#")
TRAFFIC  = Namespace("http://example.org/smartcity/traffic#")

SOSA     = Namespace("http://www.w3.org/ns/sosa/")
TIME     = Namespace("http://www.w3.org/2006/time#")
SCTIME   = Namespace("http://example.org/smartcity/time/")

g.bind("exw", EXW)
g.bind("excore", EXCORE)
g.bind("extraffic", EXTRAF)
g.bind("sc", SC)
g.bind("weather", WEATHER)
g.bind("traffic", TRAFFIC)
g.bind("sosa", SOSA)
g.bind("time", TIME)
g.bind("sctime", SCTIME)

# ---------------- Paths ----------------
weather_csv = "/run/determined/workdir/weather/weather_10min_by_station.csv"   
out_path    = "/run/determined/workdir/weather/weather_context_abox_v2.2.ttl"

# ---------------- Helpers ----------------
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

def obs_uri(station_id: str, t_idx: int, property_local: str):
    return EXW[f"obs_{station_id}_{t_idx}_{property_local}"]

# ---------------- Shared targets ----------------
# Kept here so the graph is self-contained.
# If you always load a separate shared-targets graph, these can remain duplicated safely.
g.add((EXCORE.darmstadt, RDF.type, SC.City))
g.add((EXCORE.darmstadt, RDFS.label, Literal("Darmstadt", lang="en")))

g.add((EXTRAF.darmstadt_traffic_network, RDF.type, TRAFFIC.TrafficNetwork))
g.add((EXTRAF.darmstadt_traffic_network, RDFS.label, Literal("Darmstadt traffic network", lang="en")))
g.add((EXTRAF.darmstadt_traffic_network, SC.locatedIn, EXCORE.darmstadt))

# ---------------- Read source weather CSV ----------------
df = pd.read_csv(weather_csv, sep=",", encoding="utf-8", low_memory=False)

# BOM fixes
if "ï»¿datetime" in df.columns and "datetime" not in df.columns:
    df = df.rename(columns={"ï»¿datetime": "datetime"})
if "ï»¿StationID" in df.columns and "StationID" not in df.columns:
    df = df.rename(columns={"ï»¿StationID": "StationID"})

# ---------------- Build one weather context per timestamp ----------------
time_inst_added = set()
ctx_added = set()

property_map = {
    "temperature": "Temperature",
    "humidity": "Humidity",
    "pressure": "Pressure",
    "precipitation": "Precipitation",
    "wind_speed": "WindSpeed",
    "windDirection": "WindDirection",
    "wind_direction": "WindDirection",
}

for _, row in df.iterrows():
    sid = str(row.get("StationID", "")).strip()
    if not sid:
        continue

    t_idx = ensure_timestamp_seconds(row)
    if t_idx is None:
        continue

    ctx   = EXW[f"ctx_weather_{t_idx}"]
    tinst = SCTIME[f"t_{t_idx}"]

    # Ensure time instant exists in this graph too
    if t_idx not in time_inst_added:
        g.add((tinst, RDF.type, TIME.Instant))
        g.add((tinst, TIME.inXSDDateTime, Literal(epoch_to_iso(t_idx), datatype=XSD.dateTime)))
        time_inst_added.add(t_idx)

    # Add the context resource once per timestamp
    if t_idx not in ctx_added:
        g.add((ctx, RDF.type, WEATHER.CityWeatherContext))
        g.add((ctx, SC.contextForTime, tinst))

        g.add((ctx, SC.appliesTo, EXCORE.darmstadt))
        g.add((ctx, SC.appliesTo, EXTRAF.darmstadt_traffic_network))

        g.add((ctx, SC.applicableSpatialScale, Literal("city-wide", datatype=XSD.string)))
        g.add((ctx, SC.spatialRepresentativeness, Literal(
            "City-wide meteorological context for traffic analysis; not an intersection-specific microclimate estimate.",
            datatype=XSD.string
        )))
        g.add((ctx, SC.derivationMethod, Literal(
            "Direct contextualization from station-level weather observations at the same timestamp.",
            datatype=XSD.string
        )))

        ctx_added.add(t_idx)

    # Provenance links to raw observations from the raw weather ABox
    for csv_col, local_name in property_map.items():
        if csv_col in row and pd.notna(row[csv_col]):
            g.add((ctx, SC.derivedFromObservation, obs_uri(sid, t_idx, local_name)))

# ---------------- Serialize ----------------
g.serialize(destination=out_path, format="turtle")
print("Saved to:", out_path)
print("Triples:", len(g))
# pip install rdflib pandas

import pandas as pd, urllib.parse
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, XSD
import datetime as dt

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
g.bind("time", TIME)
g.bind("sosa", SOSA)
g.bind("geo", GEO)
g.bind("qudt", QUDT)
g.bind("unit", UNIT)

# Station
station_id = "Darmstadt_1"
lat, lon   = 49.881, 8.678
station    = EX[f"station_{station_id}"]
sensor     = EX[f"sensor_{station_id}"]
geom       = EX[f"geom_{station_id}"]

g.add((sensor, RDF.type, WEATHER.WeatherSensor))
g.add((sensor, SOSA.isHostedBy, station))
g.add((station, SOSA.hosts, sensor))
g.add((station, RDF.type, WEATHER.WeatherPlatform))
g.add((station, GEO.hasGeometry, geom))
g.add((geom, GEO.asWKT, Literal(f"POINT({lon} {lat})", datatype=GEO.wktLiteral)))

file_path  = "/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/weather_10min.csv"
chunk_size = 1000

time_inst_added = set()
weather_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size)

def epoch_seconds_from_ts(ts: pd.Timestamp) -> int:
    # ts already utc-aware
    return int(ts.timestamp())

for chunk in weather_chunks:
    triples = []

    for _, row in chunk.iterrows():
        ts = pd.to_datetime(str(row.get("datetime")), utc=True, errors="coerce")
        if pd.isna(ts):
            continue

        iso_t = ts.isoformat()
        tkey  = urllib.parse.quote_plus(iso_t)
        tinst = EX[f"t_{tkey}"]

        if tkey not in time_inst_added:
            triples.append((tinst, RDF.type, TIME.Instant))
            triples.append((tinst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(tkey)

        t_idx = epoch_seconds_from_ts(ts)

        def add_obs(prop_uri, val, xsd_dt, unit_uri=None):
            oname = str(prop_uri).split("#")[-1].split("/")[-1]
            obs   = EX[f"obs_{station_id}_{tkey}_{oname}"]

            triples.extend([
                (obs, RDF.type, SOSA.Observation),
                (obs, RDF.type, WEATHER.WeatherObservation),
                (obs, SOSA.madeBySensor, sensor),
                (obs, SOSA.observedProperty, prop_uri),

                # ✅ for prediction/causal
                (obs, SOSA.hasSimpleResult, Literal(val, datatype=xsd_dt)),

                # ✅ semantic time
                (obs, SOSA.phenomenonTime, tinst),

                # FOI (your current design uses station as FOI)
                (obs, SOSA.hasFeatureOfInterest, station),

                # ✅ for TKGE/ATiSE
                (obs, SC.observedAtTimeIndex, Literal(t_idx, datatype=XSD.long)),
            ])
            if unit_uri:
                triples.append((obs, QUDT.unit, unit_uri))

        if pd.notna(row.get("TT_10")):
            add_obs(WEATHER.Temperature, float(row["TT_10"]), XSD.double, UNIT.DEG_C)

        if pd.notna(row.get("RF_10")):
            add_obs(WEATHER.RelativeHumidity, float(row["RF_10"]), XSD.double, UNIT.PERCENT)

        if pd.notna(row.get("PP_10")):
            add_obs(WEATHER.AirPressure, float(row["PP_10"]), XSD.double, UNIT.HectoPA)

        if pd.notna(row.get("RWS_10")):
            add_obs(WEATHER.Precipitation, float(row["RWS_10"]), XSD.double, UNIT.MilliM)

        if pd.notna(row.get("wind_speed")):
            add_obs(WEATHER.WindSpeed, float(row["wind_speed"]), XSD.double, UNIT["M-PER-SEC"])

        if pd.notna(row.get("wind_direction")):
            add_obs(WEATHER.WindDirection, float(row["wind_direction"]), XSD.double, UNIT.DEG)

        if pd.notna(row.get("RWS_IND_10")):
            add_obs(WEATHER.RainFlag, bool(int(row["RWS_IND_10"])), XSD.boolean)

    for t in triples:
        g.add(t)

out_path = "/content/drive/MyDrive/Smart-city/A142_weather_ontology.ttl"
g.serialize(destination=out_path, format="turtle")
print(f"✅ Done! Total triples in weather ontology: {len(g)}")

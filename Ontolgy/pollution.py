import pandas as pd, urllib.parse, json
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

# Setup RDF Graph and Namespaces 
g = Graph()
EX    = Namespace("http://example.org/pollution/")  
POLL  = Namespace("https://w3id.org/airpollution#") 
SOSA  = Namespace("http://www.w3.org/ns/sosa/")
TIME  = Namespace("http://www.w3.org/2006/time#")
WGS84 = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
QUDT  = Namespace("http://qudt.org/schema/qudt/")
UNIT  = Namespace("http://qudt.org/vocab/unit/")

g.bind("ex", EX); g.bind("poll", POLL); g.bind("sosa", SOSA)
g.bind("time", TIME); g.bind("wgs84", WGS84); g.bind("qudt", QUDT); g.bind("unit", UNIT)

# Properties
NO2   = EX.NO2;    g.add((NO2,   RDF.type, SOSA.ObservableProperty))
PM10  = EX.PM10;   g.add((PM10,  RDF.type, SOSA.ObservableProperty))
PM25  = EX.PM2_5;  g.add((PM25,  RDF.type, SOSA.ObservableProperty))

# Metadata for pollution stations 
metadata_path = "/content/drive/MyDrive/Test ontology_A142/Pollution/pollution_stations_metadata.csv"  
metadata = pd.read_csv(metadata_path, sep=",", encoding='latin-1')

# Create pollution station and sensor nodes 
station_uri_map = {}
sensor_uri_map  = {}

for _, row in metadata.iterrows():
    sid   = str(row["StationID"]).strip()
    platform  = EX[f"station_{sid}"]   # platform = station
    sens  = EX[f"sensor_{sid}"]    # a generic sensor hosted at that station
    station_uri_map[sid] = str(platform); sensor_uri_map[sid] = str(sens)

    g.add((platform, RDF.type, SOSA.Platform))
    g.add((platform, RDFS.label, Literal(row.get("address","").strip() or f"Station {sid}")))
    # coords
    if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
        g.add((platform, WGS84.lat,  Literal(float(row["latitude"]),  datatype=XSD.decimal)))
        g.add((platform, WGS84.long, Literal(float(row["longitude"]), datatype=XSD.decimal)))
    # optional identifiers
    g.add((platform, EX.stationId, Literal(sid)))
    if pd.notna(row.get("OSM_ID")):
        g.add((platform, EX.osmId, Literal(str(row["OSM_ID"]))))

    # sensor hosted by the platform
    g.add((sens, RDF.type, SOSA.Sensor))
    g.add((sens, SOSA.isHostedBy, platform))
    g.add((platform, SOSA.hosts, sens))

# Time cache to avoid duplicates
time_inst_added = set()
# Load pollution data in chunks 
file_path = "/content/drive/MyDrive/Test ontology_A142/1 day pollution.csv"  # Correct path
chunk_size = 500
# Add encoding parameter to handle potential decoding issues for the pollution data file as well
pollution_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, decimal=',',encoding='latin-1')


for chunk in pollution_chunks:
    # normalize datetime column name if BOM present
    if "ï»¿datetime" in chunk.columns and "datetime" not in chunk.columns:
        chunk = chunk.rename(columns={"ï»¿datetime": "datetime"})

    triples = []
    for _, row in chunk.iterrows():
        # time
        ts0 = pd.to_datetime(str(row.get("datetime")), errors="coerce")
        if pd.isna(ts0):
            continue
        # convert to UTC by subtracting 1 hour, then assign UTC tz
        ts = (ts0 - pd.Timedelta(hours=1)).tz_localize("UTC")
        iso_t = ts.isoformat()
        tkey  = urllib.parse.quote_plus(iso_t)
        tinst = EX[f"t_{tkey}"]
        if tkey not in time_inst_added:
            triples.append((tinst, RDF.type, TIME.Instant))
            triples.append((tinst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(tkey)

        sid = str(row.get("StationID")).strip()
        platform_uri  = station_uri_map.get(sid)
        sens_uri  = sensor_uri_map.get(sid)
        if not platform_uri or not sens_uri:
            continue
        platform = URIRef(platform_uri); sens = URIRef(sens_uri)

        # helper to mint one obs
        def add_obs(prop_uri, val, unit_uri=None):
            oname = str(prop_uri).split("/")[-1]
            obs   = EX[f"obs_{sid}_{tkey}_{oname}"]
            triples.extend([
                (obs, RDF.type, SOSA.Observation),
                (obs, RDF.type, POLL.PollutionObservation), # custom type for pollution
                (obs, SOSA.madeBySensor, sens),
                (obs, SOSA.observedProperty, prop_uri),
                (obs, SOSA.hasSimpleResult, Literal(val, datatype=XSD.double)),
                (obs, SOSA.phenomenonTime, tinst),
                (obs, SOSA.resultTime, Literal(iso_t, datatype=XSD.dateTime)),
                (obs, SOSA.hasFeatureOfInterest, platform),
            ])
            if unit_uri:
                triples.append((obs, QUDT.unit, unit_uri))

        # values + units (µg/m³ typical)
        if pd.notna(row.get("NO2")):
            add_obs(NO2,  float(row["NO2"]),  UNIT["MicroGM-PER-M3"])
        if pd.notna(row.get("PM10")):
            add_obs(PM10, float(row["PM10"]), UNIT["MicroGM-PER-M3"])
        # note: CSV may have "PM2.5" column name
        pm25_col = "PM2.5" if "PM2.5" in row.index else "PM2_5"
        if pd.notna(row.get(pm25_col)):
            add_obs(PM25, float(row[pm25_col]), UNIT["MicroGM-PER-M3"])

    for t in triples:
        g.add(t)

# Save output 
output_path = "/content/drive/MyDrive/Smart-city/A142_pollution_ontology.ttl"
g.serialize(destination=output_path, format="turtle")
print(f"✅ Done! Total triples in pollution ontology: {len(g)}")
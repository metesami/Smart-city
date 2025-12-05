import pandas as pd, urllib.parse, json
from decimal import Decimal, InvalidOperation
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD
import re

# Setup RDF Graph and Namespaces 
g = Graph()
EX    = Namespace("http://example.org/pollution/") 
SC = Namespace("http://example.org/smartcity/core#")
POLLUTION  = Namespace("http://example.org/smartcity/pollution#") 
SOSA  = Namespace("http://www.w3.org/ns/sosa/")
TIME  = Namespace("http://www.w3.org/2006/time#")
GEO    = Namespace("http://www.opengis.net/ont/geosparql#")
QUDT  = Namespace("http://qudt.org/schema/qudt/")
UNIT  = Namespace("http://qudt.org/vocab/unit/")

g.bind("ex", EX); g.bind("pollution", POLLUTION); g.bind("sosa", SOSA),g.bind("sc", SC),
g.bind("time", TIME); g.bind("geo", GEO); g.bind("qudt", QUDT); g.bind("unit", UNIT)


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

    g.add((platform, RDF.type, POLLUTION.PollutionPlatform))
    g.add((platform, RDFS.label, Literal(row.get("address","").strip() or f"Station {sid}")))
    # coords
  
    if pd.notna(row.get("latitude")) and pd.notna(row.get("longitude")):
        lat = Decimal(row["latitude"])
        lon = Decimal(row["longitude"])

        geom = EX[f"geom_station_{sid}"]  # a URI for the geometry node
        g.add((platform, GEO.hasGeometry, geom))

        # WKT uses lon lat 
        wkt = f"POINT({lon} {lat})"
        g.add((geom, RDF.type, GEO.Geometry))
        g.add((geom, GEO.asWKT, Literal(wkt, datatype=GEO.wktLiteral)))
        g.add((platform, POLLUTION.stationLatitude,  Literal(lat, datatype=XSD.decimal)))
        g.add((platform, POLLUTION.stationLongitude, Literal(lon, datatype=XSD.decimal)))
    # optional identifiers
    g.add((platform, POLLUTION.stationId, Literal(sid)))
    osm_id = int(row["OSM_ID"])
    g.add((platform, POLLUTION.osmNodeId, Literal(osm_id, datatype=XSD.long)))
        

    # sensor hosted by the platform
    g.add((sens, RDF.type, POLLUTION.PollutionSensor))
    g.add((sens, SOSA.isHostedBy, platform))
    g.add((platform, SOSA.hosts, sens))

# Time cache to avoid duplicates
time_inst_added = set()
# Load pollution data in chunks 
file_path = "/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/pollution_10min.csv"  # Correct path
chunk_size = 500
# Add encoding parameter to handle potential decoding issues for the pollution data file as well
pollution_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, decimal=',',encoding='latin-1')

'''
# Helper to create URIs from labels
def label_to_bin_uri(label, var_prefix="PM25"):
    s = re.sub(r"[^\w\-_\.]", "_", label.strip())
    return URIRef(str(POLLUTION) + s)
'''

only_stationID = "DEHE040"
pollution_bins = []
for chunk in pollution_chunks:
    # normalize datetime column name if BOM present
    if "ï»¿datetime" in chunk.columns and "datetime" not in chunk.columns:
        chunk = chunk.rename(columns={"ï»¿datetime": "datetime"})

    triples = []
    for _, row in chunk.iterrows():
        # time
        '''
        ts = pd.to_datetime(str(row["datetime"]), utc=True, errors="coerce")
        iso_t = ts.isoformat()
        tkey  = urllib.parse.quote_plus(iso_t)
        tinst = EX[f"t_{tkey}"]
        if tkey not in time_inst_added:
            triples.append((tinst, RDF.type, TIME.Instant))
            triples.append((tinst, TIME.inXSDDateTime, Literal(iso_t, datatype=XSD.dateTime)))
            time_inst_added.add(tkey)
        '''

        sid = str(only_stationID)
        platform_uri  = station_uri_map.get(sid)
        sens_uri  = sensor_uri_map.get(sid)
        if not platform_uri or not sens_uri:
            continue
        platform = URIRef(platform_uri); sens = URIRef(sens_uri)

        ts_seconds_val = row.get("timestamp_seconds")

        try:
            timestamp_seconds = int(ts_seconds_val) if pd.notna(ts_seconds_val) else None
        except Exception:
            timestamp_seconds = None

        # helper to mint one obs
        def add_obs(prop_uri, val, unit_uri=None,category_label=None):
            
            oname = str(prop_uri).split("/")[-1]
            obs   = EX[f"obs_{sid}_{timestamp_seconds}_{oname}"]
            triples.extend([
                (obs, RDF.type, POLLUTION.PollutionObservation),
                (obs, RDF.type, SOSA.Observation),
                (obs, SOSA.madeBySensor, sens),
                (sens, SOSA.madeObservation, obs),
                (obs, SOSA.observedProperty, prop_uri),
                (sens, SOSA.observes, prop_uri),
                (prop_uri,SOSA.isObservedBy, sens),
                #(obs, SOSA.hasSimpleResult, Literal(val, datatype=XSD.double)),
                #(obs, SOSA.phenomenonTime, tinst),
                #(obs, SOSA.resultTime, Literal(iso_t, datatype=XSD.dateTime)),
                (obs, SOSA.hasFeatureOfInterest, platform),
                (obs, SC.observedAtTimeIndex, Literal(int(timestamp_seconds), datatype=XSD.long))
            ])
            if unit_uri:
                triples.append((obs, QUDT.unit, unit_uri))
            
            # attach category individual
            if category_label:
                
                cat_uri = URIRef(str(POLLUTION) + category_label)
                triples.append((obs, POLLUTION.hasCategory, cat_uri))
                triples.append((cat_uri, POLLUTION.isCategoryOf, obs))
                sub_category_uri = POLLUTION[f"{oname}_Category"]
                if cat_uri not in pollution_bins:
                    triples.append((cat_uri, RDF.type, sub_category_uri))
                    triples.append(sub_category_uri, RDF.type, POLLUTION.PollutionCategory),
                    triples.append((cat_uri, RDFS.label, Literal(category_label)))
                    pollution_bins.append(cat_uri)
        # values + units (µg/m³ typical)
        if pd.notna(row.get("NO2")):
            add_obs(POLLUTION.NO2, float(row["NO2"]), UNIT["MicroGM-PER-M3"],
                    category_label=row.get("NO2_category"))
        if pd.notna(row.get("PM10")):
            add_obs(POLLUTION.PM10, float(row["PM10"]), UNIT["MicroGM-PER-M3"],
                    category_label=row.get("PM10_category"))
        # note: CSV may have "PM2.5" column name
        if pd.notna(row.get("PM2.5")):
            add_obs(POLLUTION.PM25, float(row["PM2.5"]), UNIT["MicroGM-PER-M3"],
                    category_label=row.get("PM2.5_category"))

    for t in triples:
        g.add(t)

# Save output 
output_path = "/content/drive/MyDrive/Smart-city/A142_pollution_ontology.ttl"
g.serialize(destination=output_path, format="turtle")
print(f" Done! Total triples in pollution ontology: {len(g)}")
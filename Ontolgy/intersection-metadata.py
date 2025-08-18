# pip install rdflib pandas requests

import json, time, requests, urllib.parse
import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD, DCTERMS

# --- Namespaces ---
EX       = Namespace("http://example.org/traffic/")
CTDO     = Namespace("https://w3id.org/ctdo#")
SOSA     = Namespace("http://www.w3.org/ns/sosa/")
WGS84NS  = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
DCMITYPE = Namespace("http://purl.org/dc/dcmitype/")

g = Graph()
g.bind("ex", EX)
g.bind("ctdo", CTDO)
g.bind("sosa", SOSA)
g.bind("wgs84", WGS84NS)
g.bind("geo",   WGS84NS)   # alias
g.bind("dcterms", DCTERMS)
g.bind("dcmitype", DCMITYPE)

# --- Config ---
OSM_NODE_API    = "https://api.openstreetmap.org/api/0.6/node/{nid}.json"
file_path       = "/content/drive/MyDrive/Test ontology_A142/A142_L5_20230901_complete.csv"
intersection_id = "A142"

# --- Helpers ---
def clean_osm_id(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
        return None
    s = str(v).strip()
    try:
        s = str(int(float(s)))
    except Exception:
        pass
    return s

def fetch_node_latlon(node_id, session=None, timeout=25):
    url = OSM_NODE_API.format(nid=int(node_id))
    s = session or requests.Session()
    headers = {"User-Agent": "SmartCity-KG/1.0 (contact: you@example.org)"}
    r = s.get(url, timeout=timeout, headers=headers)
    r.raise_for_status()
    js = r.json()
    els = js.get("elements", [])
    if not els:
        return None
    el = els[0]
    return float(el["lat"]), float(el["lon"])

def parse_bool(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().lower()
    true_set  = {"yes", "ja", "true", "1"}
    false_set = {"no",  "nein", "false", "0"}
    if s in true_set:  return True
    if s in false_set: return False
    return None

# --- Load data ---
# try utf-8 first; fall back to latin-1 if needed
try:
    metadata_df = pd.read_csv(file_path)
except UnicodeDecodeError:
    metadata_df = pd.read_csv(file_path, encoding="latin-1")

# --- Create intersection ---
intersection_uri = EX[f"intersection_{intersection_id}"]
g.add((intersection_uri, RDF.type, CTDO.Intersection))
g.add((intersection_uri, RDFS.label, Literal(f"Intersection {intersection_id}", lang="en")))

# --- Fetch unique node coords for intersection & attach ---
session    = requests.Session()
node_cache = {}
node_ids   = set()

if "osm_node_id" in metadata_df.columns:
    for v in metadata_df["osm_node_id"].dropna():
        nid = clean_osm_id(v)
        if nid:
            node_ids.add(nid)

for nid in sorted(node_ids):
    if nid in node_cache:
        latlon = node_cache[nid]
    else:
        try:
            latlon = fetch_node_latlon(nid, session=session)
            node_cache[nid] = latlon
            time.sleep(0.4)  # polite to OSM
        except Exception:
            latlon = None
    if not latlon:
        continue
    lat, lon = latlon
    g.add((intersection_uri, WGS84NS.lat,  Literal(lat, datatype=XSD.decimal)))
    g.add((intersection_uri, WGS84NS.long, Literal(lon, datatype=XSD.decimal)))

# --- Streets, Lanes, Sensors ---
sensor_uri_map     = {}
street_uri_map     = {}
lane_uri_map       = {}
sensor_to_lane_map = {}

for _, row in metadata_df.iterrows():
    sid         = str(row.get("sensor_id", "")).strip()
    way_v       = row.get("way_id", None)
    node_v      = row.get("osm_node_id", None)
    if pd.isna(way_v) or str(way_v).strip() == "":
        continue

    way_id       = clean_osm_id(way_v)      # strings
    osm_node_id  = clean_osm_id(node_v)     # strings
    lane_index   = row.get("lane_index(0-based from left to right)", None)
    turn_dir     = row.get("turn_direction", None)
    det_type     = row.get("detector_type", None)
    road_name    = str(row.get("road_name", "unknown")).strip()
    conn_roads   = row.get("connected_roads", None)
    det_range    = row.get("Sensor_detection_range(m)", None)
    dist_stop    = row.get("sensor_distance_to_stopline(m)", None)
    bicycle_lane = row.get("bicycle_dedicated_lane", None)

    # Street (per way_id)
    if way_id not in street_uri_map:
        street_uri = EX[f"way_{urllib.parse.quote_plus(way_id)}"]
        street_uri_map[way_id] = street_uri

        g.add((street_uri, RDF.type, CTDO.Street))
        g.add((street_uri, CTDO.wayId,     Literal(way_id, datatype=XSD.string)))
        if osm_node_id:
            g.add((street_uri, CTDO.osmNodeId, Literal(osm_node_id, datatype=XSD.string)))
        if road_name:
            g.add((street_uri, RDFS.label, Literal(road_name, lang="de")))

        g.add((intersection_uri, CTDO.hasStreet, street_uri))
        g.add((street_uri, CTDO.connectsToIntersection, intersection_uri))

        # Attach street-level coords via its node (optional)
        if osm_node_id:
            latlon = node_cache.get(osm_node_id)
            if latlon is None:
                try:
                    latlon = fetch_node_latlon(osm_node_id, session=session)
                    node_cache[osm_node_id] = latlon
                    time.sleep(0.4)
                except Exception:
                    latlon = None
            if latlon:
                lat, lon = latlon
                g.add((street_uri, WGS84NS.lat,  Literal(lat, datatype=XSD.decimal)))
                g.add((street_uri, WGS84NS.long, Literal(lon, datatype=XSD.decimal)))
    else:
        street_uri = street_uri_map[way_id]

    # Lane
    lane_uri = EX[f"lane_{intersection_id}_{way_id}_{lane_index}"]
    g.add((lane_uri, RDF.type, CTDO.Lane))
    if lane_index is not None and not pd.isna(lane_index):
        g.add((lane_uri, CTDO.laneIndex, Literal(int(lane_index), datatype=XSD.integer)))
    if turn_dir:
        g.add((lane_uri, CTDO.turnDirection, Literal(str(turn_dir))))
    if osm_node_id:
        g.add((lane_uri, CTDO.osmNodeId, Literal(osm_node_id, datatype=XSD.string)))

    val = parse_bool(bicycle_lane)
    if val is not None:
        g.add((lane_uri, CTDO.bicycleDedicatedLane, Literal(val, datatype=XSD.boolean)))

    g.add((lane_uri, CTDO.belongsToStreet, street_uri))
    g.add((lane_uri, CTDO.atIntersection, intersection_uri))
    lane_uri_map[f"{intersection_id}:{way_id}:{lane_index}"] = str(lane_uri)

    # Sensor
    if sid:
        sensor_uri = EX[f"sensor_{urllib.parse.quote_plus(sid)}"]
        g.add((sensor_uri, RDF.type, SOSA.Sensor))
        g.add((sensor_uri, RDF.type, CTDO.TrafficSensor))
        g.add((sensor_uri, CTDO.hasSensorId, Literal(sid)))
        if det_type and not pd.isna(det_type):
            g.add((sensor_uri, CTDO.detectorType, Literal(str(det_type))))
        if det_range is not None and not pd.isna(det_range):
            g.add((sensor_uri, CTDO.detectionRange, Literal(float(det_range), datatype=XSD.float)))
        if dist_stop is not None and not pd.isna(dist_stop):
            g.add((sensor_uri, CTDO.distanceToStopline, Literal(float(dist_stop), datatype=XSD.float)))
        if conn_roads and not pd.isna(conn_roads):
            g.add((sensor_uri, CTDO.connectedRoads, Literal(str(conn_roads))))
        g.add((sensor_uri, CTDO.detectsTrafficOn, lane_uri))

        sensor_uri_map[sid] = str(sensor_uri)
        sensor_to_lane_map[sid] = str(lane_uri)

# --- Dataset metadata & save maps ---
dataset_uri = EX[f"dataset_intersection_{intersection_id}"]
g.add((dataset_uri, RDF.type, DCMITYPE.Dataset))
g.add((dataset_uri, RDFS.label, Literal(f"Intersection {intersection_id} metadata", lang="en")))
g.add((dataset_uri, DCTERMS.source, Literal(file_path)))

SENSOR_MAP_JSON  = "/content/drive/MyDrive/Smart-city/sensor_uri_map.json"
LANE_MAP_JSON    = "/content/drive/MyDrive/Smart-city/lane_uri_map.json"
SENSOR2LANE_JSON = "/content/drive/MyDrive/Smart-city/sensor_to_lane_map.json"

with open(SENSOR_MAP_JSON, "w") as f:
    json.dump(sensor_uri_map, f, ensure_ascii=False, indent=2)
with open(LANE_MAP_JSON, "w") as f:
    json.dump(lane_uri_map, f, ensure_ascii=False, indent=2)
with open(SENSOR2LANE_JSON, "w") as f:
    json.dump(sensor_to_lane_map, f, ensure_ascii=False, indent=2)

output_path = "/content/drive/MyDrive/Smart-city/A142_intersection_ontology.ttl"
g.serialize(destination=output_path, format="turtle")
print("✅ Done! Triples:", len(g))
print("JSON maps written:",
      "\n  sensors     :", len(sensor_uri_map),
      "\n  lanes       :", len(lane_uri_map),
      "\n  sensor→lane :", len(sensor_to_lane_map))

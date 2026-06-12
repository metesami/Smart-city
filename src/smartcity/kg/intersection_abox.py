from pathlib import Path
import json
import time
import urllib.parse

import pandas as pd
import requests
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD, DCTERMS


EX = Namespace("http://example.org/traffic/")
SOSA = Namespace("http://www.w3.org/ns/sosa/")
DCMITYPE = Namespace("http://purl.org/dc/dcmitype/")
SC = Namespace("http://example.org/smartcity/core#")
TRAFFIC = Namespace("http://example.org/smartcity/traffic#")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")

OSM_NODE_API = "https://api.openstreetmap.org/api/0.6/node/{nid}.json"


def clean_osm_id(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == "":
        return None
    s = str(v).strip()
    try:
        s = str(int(float(s)))
    except Exception:
        pass
    return s


def parse_bool(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().lower()
    if s in {"yes", "ja", "true", "1"}:
        return True
    if s in {"no", "nein", "false", "0"}:
        return False
    return None


def fetch_node_latlon(node_id, session=None, timeout=25):
    url = OSM_NODE_API.format(nid=int(node_id))
    s = session or requests.Session()
    headers = {"User-Agent": "SmartCity-KG/1.0"}
    r = s.get(url, timeout=timeout, headers=headers)
    r.raise_for_status()
    js = r.json()
    els = js.get("elements", [])
    if not els:
        return None
    el = els[0]
    return float(el["lat"]), float(el["lon"])


def attach_point_geometry(graph, subject_uri, lat, lon, geom_suffix="_geom_main"):
    geom = URIRef(str(subject_uri) + geom_suffix)
    wkt = f"POINT({lon} {lat})"
    graph.add((subject_uri, SC.hasGeometry, geom))
    graph.add((geom, RDF.type, GEO.Geometry))
    graph.add((geom, SC.asWKT, Literal(wkt, datatype=GEO.wktLiteral)))
    return geom


def build_intersection_abox(
    metadata_file: str | Path,
    intersection_id: str,
    output_ttl: str | Path,
    sensor_map_json: str | Path,
    lane_map_json: str | Path,
    sensor_to_lane_json: str | Path,
    fetch_osm: bool = True,
) -> Path:
    metadata_file = Path(metadata_file)
    output_ttl = Path(output_ttl)
    sensor_map_json = Path(sensor_map_json)
    lane_map_json = Path(lane_map_json)
    sensor_to_lane_json = Path(sensor_to_lane_json)

    output_ttl.parent.mkdir(parents=True, exist_ok=True)
    sensor_map_json.parent.mkdir(parents=True, exist_ok=True)

    graph = Graph()
    graph.bind("ex", EX)
    graph.bind("sosa", SOSA)
    graph.bind("dcterms", DCTERMS)
    graph.bind("dcmitype", DCMITYPE)
    graph.bind("sc", SC)
    graph.bind("traffic", TRAFFIC)
    graph.bind("geo", GEO)

    if metadata_file.suffix in [".xlsx", ".xls"]:
        metadata_df = pd.read_excel(metadata_file)
    else:
        try:
            metadata_df = pd.read_csv(metadata_file)
        except UnicodeDecodeError:
            metadata_df = pd.read_csv(metadata_file, encoding="latin-1")

    intersection_uri = EX[f"intersection_{intersection_id}"]
    graph.add((intersection_uri, RDF.type, TRAFFIC.Intersection))
    graph.add((intersection_uri, RDFS.label, Literal(f"Intersection {intersection_id}", lang="en")))
    graph.add((intersection_uri, TRAFFIC.intersectionId, Literal(intersection_id, datatype=XSD.string)))

    session = requests.Session()
    node_cache = {}
    node_ids = set()

    if fetch_osm and "osm_node_id" in metadata_df.columns:
        for v in metadata_df["osm_node_id"].dropna():
            nid = clean_osm_id(v)
            if nid:
                node_ids.add(nid)

    coords = []
    for nid in sorted(node_ids):
        try:
            latlon = fetch_node_latlon(nid, session=session)
            node_cache[nid] = latlon
            time.sleep(0.35)
        except Exception:
            latlon = None
        if latlon:
            coords.append(latlon)

    if coords:
        lat_c = sum(lat for lat, lon in coords) / len(coords)
        lon_c = sum(lon for lat, lon in coords) / len(coords)
        attach_point_geometry(graph, intersection_uri, lat_c, lon_c)

    sensor_uri_map = {}
    street_uri_map = {}
    lane_uri_map = {}
    sensor_to_lane_map = {}

    for _, row in metadata_df.iterrows():
        sid = str(row.get("sensor_id", "")).strip()

        way_v = row.get("way_id", None)
        node_v = row.get("osm_node_id", None)

        if pd.isna(way_v) or str(way_v).strip() == "":
            continue

        way_id = clean_osm_id(way_v)
        osm_node_id = clean_osm_id(node_v)

        lane_index = row.get("lane_index(0-based from left to right)", None)
        turn_dir = row.get("turn_direction", None)
        det_type = row.get("detector_type", None)
        road_name = str(row.get("road_name", "unknown")).strip()
        conn_roads = row.get("connected_roads", None)
        det_range = row.get("Sensor_detection_range(m)", None)
        dist_stop = row.get("sensor_distance_to_stopline(m)", None)
        bicycle_lane = row.get("bicycle_dedicated_lane", None)

        if way_id not in street_uri_map:
            street_uri = EX[f"way_{urllib.parse.quote_plus(way_id)}"]
            street_uri_map[way_id] = street_uri

            graph.add((street_uri, RDF.type, TRAFFIC.Street))
            graph.add((street_uri, TRAFFIC.osmWayId, Literal(way_id, datatype=XSD.string)))

            if osm_node_id:
                graph.add((street_uri, TRAFFIC.osmNodeId, Literal(osm_node_id, datatype=XSD.string)))

            if road_name:
                graph.add((street_uri, RDFS.label, Literal(road_name, lang="de")))

            graph.add((intersection_uri, TRAFFIC.hasStreet, street_uri))
            graph.add((street_uri, TRAFFIC.streetOf, intersection_uri))
        else:
            street_uri = street_uri_map[way_id]

        lane_idx_str = str(lane_index) if lane_index is not None and not pd.isna(lane_index) else "na"
        lane_uri = EX[f"lane_{intersection_id}_{way_id}_{lane_idx_str}"]

        graph.add((lane_uri, RDF.type, TRAFFIC.Lane))

        if lane_index is not None and not pd.isna(lane_index):
            graph.add((lane_uri, TRAFFIC.laneIndex, Literal(int(lane_index), datatype=XSD.integer)))

        if turn_dir is not None and not pd.isna(turn_dir):
            graph.add((lane_uri, TRAFFIC.turnDirection, Literal(str(turn_dir))))

        if osm_node_id:
            graph.add((lane_uri, TRAFFIC.osmNodeId, Literal(osm_node_id, datatype=XSD.string)))

        val = parse_bool(bicycle_lane)
        if val is not None:
            graph.add((lane_uri, TRAFFIC.bicycleDedicatedLane, Literal(val, datatype=XSD.boolean)))

        graph.add((lane_uri, TRAFFIC.isLaneOf, street_uri))
        graph.add((lane_uri, TRAFFIC.laneBelongsToIntersection, intersection_uri))

        lane_uri_map[f"{intersection_id}:{way_id}:{lane_idx_str}"] = str(lane_uri)

        if sid:
            sensor_uri = EX[f"sensor_{urllib.parse.quote_plus(sid)}"]
            graph.add((sensor_uri, RDF.type, TRAFFIC.TrafficSensor))
            graph.add((sensor_uri, TRAFFIC.sensorId, Literal(sid)))

            if det_type is not None and not pd.isna(det_type):
                graph.add((sensor_uri, TRAFFIC.detectorType, Literal(str(det_type))))

            if det_range is not None and not pd.isna(det_range):
                graph.add((sensor_uri, TRAFFIC.detectionRange, Literal(float(det_range), datatype=XSD.float)))

            if dist_stop is not None and not pd.isna(dist_stop):
                graph.add((sensor_uri, TRAFFIC.distanceToStopline, Literal(float(dist_stop), datatype=XSD.float)))

            if conn_roads is not None and not pd.isna(conn_roads):
                graph.add((lane_uri, TRAFFIC.connectedRoads, Literal(str(conn_roads))))

            graph.add((sensor_uri, TRAFFIC.detectsTrafficOn, lane_uri))
            graph.add((lane_uri, TRAFFIC.hasSensor, sensor_uri))

            sensor_uri_map[sid] = str(sensor_uri)
            sensor_to_lane_map[sid] = str(lane_uri)

    dataset_uri = EX[f"dataset_intersection_{intersection_id}"]
    graph.add((dataset_uri, RDF.type, DCMITYPE.Dataset))
    graph.add((dataset_uri, RDFS.label, Literal(f"Intersection {intersection_id} metadata", lang="en")))
    graph.add((dataset_uri, DCTERMS.source, Literal(str(metadata_file))))

    with open(sensor_map_json, "w", encoding="utf-8") as f:
        json.dump(sensor_uri_map, f, ensure_ascii=False, indent=2)

    with open(lane_map_json, "w", encoding="utf-8") as f:
        json.dump(lane_uri_map, f, ensure_ascii=False, indent=2)

    with open(sensor_to_lane_json, "w", encoding="utf-8") as f:
        json.dump(sensor_to_lane_map, f, ensure_ascii=False, indent=2)

    graph.serialize(destination=output_ttl, format="turtle")

    print("Intersection ABox finished.")
    print(f"Triples: {len(graph)}")
    print(f"Output TTL: {output_ttl}")
    print(f"Sensors: {len(sensor_uri_map)}")
    print(f"Lanes: {len(lane_uri_map)}")

    return output_ttl
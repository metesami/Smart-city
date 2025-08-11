pip install rdflib
import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, RDFS, XSD
import urllib.parse

#  1. Setup RDF Graph and Namespaces
g = Graph()
EX = Namespace("http://example.org/traffic/")
CTDO = Namespace("https://w3id.org/ctdo#")  # City Traffic Detector Ontology
SOSA = Namespace("http://www.w3.org/ns/sosa/")
GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
g.bind("ex", EX)
g.bind("ctdo", CTDO)
g.bind("sosa", SOSA)
g.bind("geo", GEO)

#  2. Load A142 metadata from Excel
file_path = "/content/drive/MyDrive/Test ontology_A142/A142_L5_20230901_complete.csv"
metadata_df = pd.read_excel(file_path)

#  3. Create intersection node
intersection_id = "A142"
intersection_uri = EX[f"intersection_{intersection_id}"]
g.add((intersection_uri, RDF.type, CTDO.Intersection))
g.add((intersection_uri, RDFS.label, Literal(f"Intersection {intersection_id}")))

#  4. Create street, lane, and sensor nodes
sensor_uri_map = {}
street_uri_map = {}

for _, row in metadata_df.iterrows():
    sid = str(row["sensor_id"]).strip()
    lane_index = row.get("lane_index(0-based from left to right)", None)
    turn_direction = row.get("turn_direction", None)
    detector_type = row.get("detector_type", None)
    osm_node_id = row.get("osm_node_id", None)
    way_id = row.get("way_id", None)
    road_name = str(row.get("road_name", "unknown")).strip()
    connected_roads = row.get("connected_roads", None)
    detection_range = row.get("Sensor_detection_range(m)", None)
    distance_to_stopline = row.get("sensor_distance_to_stopline(m)", None)
    bicycle_lane = row.get("bicycle_dedicated_lane", None)

    #  Create Street URI (unique per road name)
    if road_name not in street_uri_map:
        street_uri = EX[f"street_{urllib.parse.quote_plus(road_name)}"]
        street_uri_map[road_name] = street_uri
        g.add((street_uri, RDF.type, CTDO.Street))
        g.add((street_uri, RDFS.label, Literal(road_name)))

        # Add OSM identifiers to street
        if osm_node_id:
            g.add((street_uri, CTDO.osmNodeId, Literal(str(osm_node_id))))
        if way_id:
            g.add((street_uri, CTDO.wayId, Literal(str(way_id))))

        # Link Street to Intersection
        g.add((intersection_uri, CTDO.hasStreet, street_uri))
        g.add((street_uri, CTDO.connectsToIntersection, intersection_uri))
    else:
        street_uri = street_uri_map[road_name]

    #  Create Lane URI
    lane_uri = EX[f"lane_{intersection_id}_{lane_index}_{urllib.parse.quote_plus(road_name)}"]
    g.add((lane_uri, RDF.type, CTDO.Lane))
    if lane_index is not None:
        g.add((lane_uri, CTDO.laneIndex, Literal(lane_index)))
    if turn_direction:
        g.add((lane_uri, CTDO.turnDirection, Literal(turn_direction)))
    if bicycle_lane is not None:
        g.add((lane_uri, CTDO.bicycleDedicatedLane, Literal(bool(bicycle_lane))))

    # Link Lane to Street
    g.add((lane_uri, CTDO.belongsToStreet, street_uri))

    #  Create Sensor URI
    sensor_uri = EX[f"sensor_{sid}"]
    sensor_uri_map[sid] = sensor_uri
    g.add((sensor_uri, RDF.type, SOSA.Sensor))
    g.add((sensor_uri, RDF.type, CTDO.TrafficSensor))  # General type for loop & video detectors
    g.add((sensor_uri, CTDO.hasSensorId, Literal(sid)))
    if detector_type:
        g.add((sensor_uri, CTDO.detectorType, Literal(detector_type)))
    if detection_range is not None:
        g.add((sensor_uri, CTDO.detectionRange, Literal(detection_range, datatype=XSD.float)))
    if distance_to_stopline is not None:
        g.add((sensor_uri, CTDO.distanceToStopline, Literal(distance_to_stopline, datatype=XSD.float)))
    if connected_roads:
        g.add((sensor_uri, CTDO.connectedRoads, Literal(connected_roads)))

    # Link Sensor to Lane
    g.add((sensor_uri, CTDO.detectsTrafficOn, lane_uri))

# Save authoritative mapping for reuse
pd.Series(sensor_uri_map).to_json("/content/sensor_uri_map.json")

#  5. Save RDF to file
output_path = "/mnt/data/A142_intersection_ontology.ttl"
g.serialize(destination=output_path, format="turtle")

print(f"✅ Done! Total triples in intersection ontology: {len(g)}")

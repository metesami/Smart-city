# pip install rdflib

from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, XSD
import re, math

SC  = Namespace("http://example.org/smartcity/core#")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")

TRAFFIC   = Namespace("http://example.org/smartcity/traffic#")
WEATHER   = Namespace("http://example.org/smartcity/weather#")
POLLUTION = Namespace("http://example.org/smartcity/pollution#")

POINT_RE = re.compile(r"POINT\(\s*([-\d.]+)\s+([-\d.]+)\s*\)")

def parse_point_wkt(wkt_str: str):
    m = POINT_RE.match(wkt_str.strip())
    if not m:
        return None
    lon = float(m.group(1))
    lat = float(m.group(2))
    return lat, lon

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def get_feature_point(g: Graph, feature_uri: URIRef):
    geoms = list(g.objects(feature_uri, SC.hasGeometry))
    if not geoms:
        return None
    # prefer *_geom_main
    geom = None
    for gg in geoms:
        if str(gg).endswith("_geom_main") or str(gg).endswith("_main"):
            geom = gg
            break
    geom = geom or geoms[0]
    wkt = g.value(geom, GEO.asWKT)
    if not wkt:
        return None
    return parse_point_wkt(str(wkt))

def collect_by_type_with_point(g: Graph, class_uri: URIRef):
    items = set(g.subjects(RDF.type, class_uri))
    pts = {}
    for u in items:
        pt = get_feature_point(g, u)
        if pt:
            pts[u] = pt
    return pts

def build_nearest_links(out: Graph, sources_pts, targets_pts, shortcut_prop, link_prefix,
                        version="spatial_v1", method="nearest"):
    for s_uri, (s_lat, s_lon) in sources_pts.items():
        best_t = None
        best_d = None
        for t_uri, (t_lat, t_lon) in targets_pts.items():
            d = haversine_m(s_lat, s_lon, t_lat, t_lon)
            if best_d is None or d < best_d:
                best_d = d
                best_t = t_uri
        if best_t is None:
            continue

        # shortcut triple
        out.add((s_uri, shortcut_prop, best_t))

        # reified SpatialLink
        link_uri = URIRef(f"{link_prefix}{s_uri.split('/')[-1]}_{best_t.split('/')[-1]}")
        out.add((link_uri, RDF.type, SC.SpatialLink))
        out.add((link_uri, SC.spatialSource, s_uri))
        out.add((link_uri, SC.spatialTarget, best_t))
        out.add((link_uri, SC.distanceMeters, Literal(float(best_d), datatype=XSD.double)))
        out.add((link_uri, SC.linkMethod, Literal(method)))
        out.add((link_uri, SC.linkVersion, Literal(version)))

# ---------------- paths ----------------
INTERSECTION_TTL = "/content/drive/MyDrive/Smart-city/A142_intersection_ontology.ttl"
WEATHER_TTL      = "/content/drive/MyDrive/Smart-city/A142_weather_ontology.ttl"
POLLUTION_TTL    = "/content/drive/MyDrive/Smart-city/A142_pollution_ontology.ttl"
OUT_TTL          = "/content/drive/MyDrive/Smart-city/A142_spatial_links.ttl"

# ---------------- load graphs ----------------
g = Graph()
g.parse(INTERSECTION_TTL, format="turtle")
g.parse(WEATHER_TTL, format="turtle")
g.parse(POLLUTION_TTL, format="turtle")

intersections = collect_by_type_with_point(g, TRAFFIC.Intersection)
weather_plats = collect_by_type_with_point(g, WEATHER.WeatherPlatform)
poll_plats    = collect_by_type_with_point(g, POLLUTION.PollutionPlatform)

out = Graph()
out.bind("sc", SC)
out.bind("geo", GEO)

build_nearest_links(
    out, intersections, weather_plats,
    SC.nearestWeatherPlatform,
    "http://example.org/smartcity/spatial/link_weather_"
)
build_nearest_links(
    out, intersections, poll_plats,
    SC.nearestPollutionPlatform,
    "http://example.org/smartcity/spatial/link_pollution_"
)

out.serialize(destination=OUT_TTL, format="turtle")
print("✅ Spatial links saved:", OUT_TTL, "triples:", len(out))
print("Intersections with point:", len(intersections),
      "| Weather platforms:", len(weather_plats),
      "| Pollution platforms:", len(poll_plats))

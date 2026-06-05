from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, RDFS

# ---------------- RDF Graph + Namespaces ----------------
g = Graph()

SC      = Namespace("http://example.org/smartcity/core#")
TRAFFIC = Namespace("http://example.org/smartcity/traffic#")
EXCORE  = Namespace("http://example.org/core/")
EXTRAF  = Namespace("http://example.org/traffic/")
EXINT   = Namespace("http://example.org/intersection/")

g.bind("sc", SC)
g.bind("traffic", TRAFFIC)
g.bind("excore", EXCORE)
g.bind("extraffic", EXTRAF)
g.bind("exint", EXINT)

# ---------------- City ----------------
city = EXCORE["darmstadt"]

g.add((city, RDF.type, SC.City))
g.add((city, RDFS.label, Literal("Darmstadt")))

# ---------------- Traffic Network ----------------
network = EXTRAF["darmstadt_traffic_network"]

g.add((network, RDF.type, TRAFFIC.TrafficNetwork))
g.add((network, RDFS.label, Literal("Darmstadt traffic network")))
g.add((network, SC.locatedIn, city))

# ---------------- Add Intersections ----------------

intersection_ids = [
"A142"
]

for iid in intersection_ids:
    inter = EXTRAF[f"intersection_{iid}"]   
    g.add((network, TRAFFIC.hasIntersection, inter))

# ---------------- Save ----------------
g.serialize("abox_shared_targets.ttl", format="turtle")

print("Shared targets ABox created.")
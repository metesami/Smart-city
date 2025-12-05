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





file_path = "/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/pollution_10min.csv"  # Correct path
chunk_size = 500
# Add encoding parameter to handle potential decoding issues for the pollution data file as well
pollution_chunks = pd.read_csv(file_path, sep=",", chunksize=chunk_size, decimal=',',encoding='latin-1')

only_stationID = "DEHE040"
pollution_bins = []

quads = []

platform_uri  = EX[f"station_{only_stationID}"] # platform = station
sens_uri  = EX[f"sensor_{only_stationID}"]# a generic sensor hosted at that station

#for every static triple, add a static timestamp
ts_static = "static"
quads.append((str(platform_uri), str(RDF.type), str(POLLUTION.PollutionPlatform), ts_static))
quads.append((str(sens_uri), str(RDF.type), str(POLLUTION.PollutionSensor), ts_static))
quads.append((str(sens_uri), str(SOSA.isHostedBy), str(platform_uri), ts_static))
quads.append((str(platform_uri), str(SOSA.hosts), str(sens_uri), ts_static))

for chunk in pollution_chunks:

    
    for _, row in chunk.iterrows():


        ts_seconds_val = row.get("timestamp_seconds")
        sid = str(only_stationID)
        try:
            timestamp_seconds = int(ts_seconds_val) if pd.notna(ts_seconds_val) else None
        except Exception:
            timestamp_seconds = None
            
        #Quadrople list

        def add_obs_optimized(prop_uri, val, category_label=None):
            if timestamp_seconds is None:
                return
            
            oname = str(prop_uri).split("#")[-1]
            obs   = EX[f"obs_{sid}_{timestamp_seconds}_{oname}"]
            ts = str(timestamp_seconds)
            
            quads.append((str(obs), str(SOSA.observedProperty), str(prop_uri), ts))

            quads.append((str(obs), str(RDF.type), str(SOSA.Observation), ts))
            quads.append((str(obs), str(SOSA.madeBySensor), str(sens_uri), ts))
            quads.append((str(sens_uri), str(SOSA.madeObservation),str(obs) , ts))
            quads.append((str(obs), str(RDF.type), str(POLLUTION.PollutionObservation), ts))
            quads.append((str(obs), str(SOSA.hasFeatureOfInterest), str(platform_uri), ts))          
            quads.append((str(obs), str(QUDT.unit), str(UNIT["MicroGM-PER-M3"]), ts))
            # Category relation pollution_bins
            if category_label:
                cat_uri = URIRef(str(POLLUTION) + category_label)
                sub_category = POLLUTION[f"{oname}_Category"]
                quads.append((str(obs), str(POLLUTION.hasCategory), str(cat_uri), ts))  
                quads.append((str(cat_uri), str(POLLUTION.isCategoryOf), str(obs), ts))
      
            if cat_uri not in pollution_bins:
                quads.append((str(cat_uri), str(RDF.type), str(sub_category), ts))
                quads.append((str(cat_uri), str(RDFS.label), category_label, ts))
                pollution_bins.append(cat_uri)

        # main loop body
        if pd.notna(row.get("NO2")):
            add_obs_optimized(POLLUTION.NO2, float(row["NO2"]), 
                            row.get("NO2_category"))

        if pd.notna(row.get("PM10")):
            add_obs_optimized(POLLUTION.PM10, float(row["PM10"]), 
                            row.get("PM10_category"))
        
        if pd.notna(row.get("PM2.5")):
            add_obs_optimized(POLLUTION.PM25, float(row["PM2.5"]), 
                            row.get("PM2.5_category"))
            


import csv


# 
with open("pollution_quads_ATiSE.tsv", "w", newline="") as f:
    writer = csv.writer(f, delimiter="\t")
    writer.writerow(["subject", "predicate", "object", "timestamp"])
    for s, p, o, t in quads:
        writer.writerow([s, p, o, t])

print(f"{len(quads)} quadro for ATiSE")

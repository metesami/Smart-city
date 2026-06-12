from pathlib import Path
import datetime as dt

import pandas as pd
from rdflib import Graph, Namespace, Literal
from rdflib.namespace import RDF, RDFS, XSD


EXP = Namespace("http://example.org/pollution/")
EXCORE = Namespace("http://example.org/core/")
EXTRAF = Namespace("http://example.org/traffic/")

SC = Namespace("http://example.org/smartcity/core#")
POLLUTION = Namespace("http://example.org/smartcity/pollution#")
TRAFFIC = Namespace("http://example.org/smartcity/traffic#")

TIME = Namespace("http://www.w3.org/2006/time#")
SCTIME = Namespace("http://example.org/smartcity/time/")


PROPERTY_MAP = {
    "NO2": "NO2",
    "PM10": "PM10",
    "PM2.5": "PM25",
}


def epoch_to_iso(ts_seconds: int) -> str:
    return dt.datetime.fromtimestamp(int(ts_seconds), tz=dt.timezone.utc).isoformat()


def ensure_timestamp_seconds(row) -> int | None:
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


def observation_uri(station_id: str, timestamp_seconds: int, property_local_name: str):
    return EXP[f"obs_{station_id}_{timestamp_seconds}_{property_local_name}"]


def build_pollution_context_abox(
    pollution_csv: str | Path,
    output_ttl: str | Path,
) -> Path:
    pollution_csv = Path(pollution_csv)
    output_ttl = Path(output_ttl)

    if not pollution_csv.exists():
        raise FileNotFoundError(f"Pollution CSV file not found: {pollution_csv}")

    output_ttl.parent.mkdir(parents=True, exist_ok=True)

    graph = Graph()

    graph.bind("exp", EXP)
    graph.bind("excore", EXCORE)
    graph.bind("extraffic", EXTRAF)
    graph.bind("sc", SC)
    graph.bind("pollution", POLLUTION)
    graph.bind("traffic", TRAFFIC)
    graph.bind("time", TIME)
    graph.bind("sctime", SCTIME)

    graph.add((EXCORE.darmstadt, RDF.type, SC.City))
    graph.add((EXCORE.darmstadt, RDFS.label, Literal("Darmstadt", lang="en")))

    graph.add((EXTRAF.darmstadt_traffic_network, RDF.type, TRAFFIC.TrafficNetwork))
    graph.add((EXTRAF.darmstadt_traffic_network, RDFS.label, Literal("Darmstadt traffic network", lang="en")))
    graph.add((EXTRAF.darmstadt_traffic_network, SC.locatedIn, EXCORE.darmstadt))

    df = pd.read_csv(pollution_csv, sep=",", encoding="utf-8", low_memory=False)

    if "ï»¿datetime" in df.columns and "datetime" not in df.columns:
        df = df.rename(columns={"ï»¿datetime": "datetime"})

    if "ï»¿StationID" in df.columns and "StationID" not in df.columns:
        df = df.rename(columns={"ï»¿StationID": "StationID"})

    time_inst_added = set()
    context_added = set()

    for _, row in df.iterrows():
        station_id = str(row.get("StationID", "")).strip()
        if not station_id:
            continue

        timestamp_seconds = ensure_timestamp_seconds(row)
        if timestamp_seconds is None:
            continue

        context = EXP[f"ctx_air_{timestamp_seconds}"]
        time_instant = SCTIME[f"t_{timestamp_seconds}"]

        if timestamp_seconds not in time_inst_added:
            graph.add((time_instant, RDF.type, TIME.Instant))
            graph.add(
                (
                    time_instant,
                    TIME.inXSDDateTime,
                    Literal(epoch_to_iso(timestamp_seconds), datatype=XSD.dateTime),
                )
            )
            time_inst_added.add(timestamp_seconds)

        if timestamp_seconds not in context_added:
            graph.add((context, RDF.type, POLLUTION.UrbanAirQualityContext))
            graph.add((context, SC.contextForTime, time_instant))
            graph.add((context, SC.appliesTo, EXTRAF.darmstadt_traffic_network))
            graph.add((context, SC.applicableSpatialScale, Literal("network-level", datatype=XSD.string)))
            graph.add(
                (
                    context,
                    SC.spatialRepresentativeness,
                    Literal(
                        "Derived from heterogeneous pollution stations and intended for traffic-network trend analysis; not a local concentration estimate for each intersection.",
                        datatype=XSD.string,
                    ),
                )
            )
            graph.add(
                (
                    context,
                    SC.derivationMethod,
                    Literal(
                        "Context assembled from station-specific pollution observations observed at the same timestamp.",
                        datatype=XSD.string,
                    ),
                )
            )
            context_added.add(timestamp_seconds)

        for csv_column, property_local_name in PROPERTY_MAP.items():
            if csv_column in row and pd.notna(row[csv_column]):
                graph.add(
                    (
                        context,
                        SC.derivedFromObservation,
                        observation_uri(station_id, timestamp_seconds, property_local_name),
                    )
                )

    graph.serialize(destination=output_ttl, format="turtle")

    print("Pollution context ABox finished.")
    print(f"Triples: {len(graph)}")
    print(f"Output: {output_ttl}")

    return output_ttl
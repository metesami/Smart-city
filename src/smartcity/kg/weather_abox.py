from pathlib import Path
from decimal import Decimal
import datetime as dt

import pandas as pd
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD


EX = Namespace("http://example.org/weather/")
SC = Namespace("http://example.org/smartcity/core#")
WEATHER = Namespace("http://example.org/smartcity/weather#")
SOSA = Namespace("http://www.w3.org/ns/sosa/")
TIME = Namespace("http://www.w3.org/2006/time#")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
QUDT = Namespace("http://qudt.org/schema/qudt/")
UNIT = Namespace("http://qudt.org/vocab/unit/")
SCTIME = Namespace("http://example.org/smartcity/time/")
EXCORE = Namespace("http://example.org/core/")


def freq_to_seconds(freq_value) -> int:
    if freq_value is None or pd.isna(freq_value):
        return 600

    freq = str(freq_value).strip().lower()

    if freq.endswith("min"):
        return int(freq.replace("min", "")) * 60

    if freq.endswith("h"):
        return int(freq.replace("h", "")) * 3600

    return 600


def dec(value):
    return Decimal(str(value).strip().replace(",", "."))


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


def add_weather_observation(
    triples: list,
    station_id: str,
    platform: URIRef,
    sensor: URIRef,
    time_instant: URIRef,
    timestamp_seconds: int,
    aggregation_window_seconds: int,
    property_uri: URIRef,
    value,
    unit_uri=None,
    xsd_datatype=XSD.double,
):
    property_name = str(property_uri).split("#")[-1].split("/")[-1]
    observation = EX[f"obs_{station_id}_{timestamp_seconds}_{property_name}"]

    triples.extend(
        [
            (observation, RDF.type, WEATHER.WeatherObservation),
            (observation, RDF.type, SOSA.Observation),
            (observation, SOSA.madeBySensor, sensor),
            (sensor, SOSA.madeObservation, observation),
            (observation, SOSA.observedProperty, property_uri),
            (sensor, SOSA.observes, property_uri),
            (property_uri, SOSA.isObservedBy, sensor),
            (observation, SOSA.hasSimpleResult, Literal(value, datatype=xsd_datatype)),
            (observation, SOSA.phenomenonTime, time_instant),
            (observation, SOSA.hasFeatureOfInterest, platform),
            (observation, SC.observedAtTimeIndex, Literal(int(timestamp_seconds), datatype=XSD.long)),
            (observation, SC.aggregationWindowSeconds, Literal(int(aggregation_window_seconds), datatype=XSD.integer)),
        ]
    )

    if unit_uri:
        triples.append((observation, QUDT.unit, unit_uri))


def build_weather_abox(
    metadata_path: str | Path,
    weather_csv: str | Path,
    output_ttl: str | Path,
    chunk_size: int = 5000,
) -> Path:
    metadata_path = Path(metadata_path)
    weather_csv = Path(weather_csv)
    output_ttl = Path(output_ttl)

    if not metadata_path.exists():
        raise FileNotFoundError(f"Weather metadata file not found: {metadata_path}")

    if not weather_csv.exists():
        raise FileNotFoundError(f"Weather CSV file not found: {weather_csv}")

    output_ttl.parent.mkdir(parents=True, exist_ok=True)

    graph = Graph()

    graph.bind("excore", EXCORE)
    graph.bind("sctime", SCTIME)
    graph.bind("ex", EX)
    graph.bind("sc", SC)
    graph.bind("weather", WEATHER)
    graph.bind("sosa", SOSA)
    graph.bind("time", TIME)
    graph.bind("geo", GEO)
    graph.bind("qudt", QUDT)
    graph.bind("unit", UNIT)

    metadata = pd.read_csv(metadata_path, sep=",", encoding="utf-8", low_memory=False)

    station_uri_map = {}
    sensor_uri_map = {}

    for _, row in metadata.iterrows():
        station_id = str(row.get("StationID", "")).strip()
        if not station_id:
            continue

        platform = EX[f"station_{station_id}"]
        sensor = EX[f"sensor_{station_id}"]

        station_uri_map[station_id] = str(platform)
        sensor_uri_map[station_id] = str(sensor)

        graph.add((platform, RDF.type, WEATHER.WeatherPlatform))
        graph.add((platform, SC.locatedIn, EXCORE["darmstadt"]))
        graph.add((platform, RDFS.label, Literal(str(row.get("address", "")).strip() or f"Weather Station {station_id}")))
        graph.add((platform, WEATHER.stationId, Literal(station_id, datatype=XSD.string)))

        address = str(row.get("address", "")).strip()
        if address:
            graph.add((platform, WEATHER.stationAddress, Literal(address, datatype=XSD.string)))

        lat = row.get("latitude")
        lon = row.get("longitude")

        if pd.notna(lat) and pd.notna(lon):
            lat_d = dec(lat)
            lon_d = dec(lon)

            geom = EX[f"station_{station_id}_geom_main"]
            graph.add((platform, SC.hasGeometry, geom))
            graph.add((geom, RDF.type, GEO.Geometry))
            graph.add((geom, SC.asWKT, Literal(f"POINT({lon_d} {lat_d})", datatype=GEO.wktLiteral)))

        if "OSM_ID" in metadata.columns:
            osm_id = row.get("OSM_ID")
            if pd.notna(osm_id):
                graph.add((platform, WEATHER.osmNodeId, Literal(str(osm_id), datatype=XSD.string)))

        graph.add((sensor, RDF.type, WEATHER.WeatherSensor))
        graph.add((sensor, SOSA.isHostedBy, platform))
        graph.add((platform, SOSA.hosts, sensor))

    time_inst_added = set()

    chunks = pd.read_csv(
        weather_csv,
        sep=",",
        chunksize=chunk_size,
        encoding="utf-8",
        low_memory=False,
    )

    for chunk in chunks:
        if "ï»¿datetime" in chunk.columns and "datetime" not in chunk.columns:
            chunk = chunk.rename(columns={"ï»¿datetime": "datetime"})

        if "ï»¿StationID" in chunk.columns and "StationID" not in chunk.columns:
            chunk = chunk.rename(columns={"ï»¿StationID": "StationID"})

        triples = []

        for _, row in chunk.iterrows():
            station_id = str(row.get("StationID", "")).strip()
            if not station_id:
                continue

            platform_uri = station_uri_map.get(station_id)
            sensor_uri = sensor_uri_map.get(station_id)

            if not platform_uri or not sensor_uri:
                continue

            platform = URIRef(platform_uri)
            sensor = URIRef(sensor_uri)

            timestamp_seconds = ensure_timestamp_seconds(row)
            if timestamp_seconds is None:
                continue

            time_instant = SCTIME[f"t_{timestamp_seconds}"]
            aggregation_window_seconds = freq_to_seconds(row.get("freq"))

            if timestamp_seconds not in time_inst_added:
                triples.append((time_instant, RDF.type, TIME.Instant))
                triples.append(
                    (
                        time_instant,
                        TIME.inXSDDateTime,
                        Literal(epoch_to_iso(timestamp_seconds), datatype=XSD.dateTime),
                    )
                )
                time_inst_added.add(timestamp_seconds)

            if pd.notna(row.get("temperature")):
                add_weather_observation(
                    triples,
                    station_id,
                    platform,
                    sensor,
                    time_instant,
                    timestamp_seconds,
                    aggregation_window_seconds,
                    WEATHER.Temperature,
                    float(row["temperature"]),
                    UNIT.DEG_C,
                    XSD.double,
                )

            if pd.notna(row.get("humidity")):
                add_weather_observation(
                    triples,
                    station_id,
                    platform,
                    sensor,
                    time_instant,
                    timestamp_seconds,
                    aggregation_window_seconds,
                    WEATHER.Humidity,
                    float(row["humidity"]),
                    UNIT.PERCENT,
                    XSD.double,
                )

            if pd.notna(row.get("pressure")):
                add_weather_observation(
                    triples,
                    station_id,
                    platform,
                    sensor,
                    time_instant,
                    timestamp_seconds,
                    aggregation_window_seconds,
                    WEATHER.Pressure,
                    float(row["pressure"]),
                    UNIT.HectoPA,
                    XSD.double,
                )

            if pd.notna(row.get("precipitation")):
                add_weather_observation(
                    triples,
                    station_id,
                    platform,
                    sensor,
                    time_instant,
                    timestamp_seconds,
                    aggregation_window_seconds,
                    WEATHER.Precipitation,
                    float(row["precipitation"]),
                    UNIT.MilliM,
                    XSD.double,
                )

            if pd.notna(row.get("wind_speed")):
                add_weather_observation(
                    triples,
                    station_id,
                    platform,
                    sensor,
                    time_instant,
                    timestamp_seconds,
                    aggregation_window_seconds,
                    WEATHER.WindSpeed,
                    float(row["wind_speed"]),
                    UNIT["M-PER-SEC"],
                    XSD.double,
                )

            if pd.notna(row.get("wind_direction")):
                add_weather_observation(
                    triples,
                    station_id,
                    platform,
                    sensor,
                    time_instant,
                    timestamp_seconds,
                    aggregation_window_seconds,
                    WEATHER.WindDirection,
                    float(row["wind_direction"]),
                    UNIT.DEG,
                    XSD.double,
                )

            if "rain_flag" in chunk.columns and pd.notna(row.get("rain_flag")):
                try:
                    add_weather_observation(
                        triples,
                        station_id,
                        platform,
                        sensor,
                        time_instant,
                        timestamp_seconds,
                        aggregation_window_seconds,
                        WEATHER.RainFlag,
                        bool(int(row["rain_flag"])),
                        None,
                        XSD.boolean,
                    )
                except Exception:
                    pass

        for triple in triples:
            graph.add(triple)

    graph.serialize(destination=output_ttl, format="turtle")

    print("Weather ABox finished.")
    print(f"Triples: {len(graph)}")
    print(f"Output: {output_ttl}")

    return output_ttl
from pathlib import Path
import json
import gzip
import math
import urllib.parse
import datetime as dt

import duckdb


NS_EX = "http://example.org/traffic/"
NS_SC = "http://example.org/smartcity/core#"
NS_TRAFFIC = "http://example.org/smartcity/traffic#"
NS_SOSA = "http://www.w3.org/ns/sosa/"
NS_TIME = "http://www.w3.org/2006/time#"
NS_SCTIME = "http://example.org/smartcity/time/"
NS_XSD = "http://www.w3.org/2001/XMLSchema#"

URI_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

URI_SOSA_OBSERVATION = f"<{NS_SOSA}Observation>"
URI_SOSA_MADE_BY_SENSOR = f"<{NS_SOSA}madeBySensor>"
URI_SOSA_OBSERVED_PROPERTY = f"<{NS_SOSA}observedProperty>"
URI_SOSA_HAS_SIMPLE_RESULT = f"<{NS_SOSA}hasSimpleResult>"
URI_SOSA_PHENOMENON_TIME = f"<{NS_SOSA}phenomenonTime>"
URI_SOSA_HAS_FEATURE_INTEREST = f"<{NS_SOSA}hasFeatureOfInterest>"

URI_TIME_INSTANT = f"<{NS_TIME}Instant>"
URI_TIME_IN_XSD_DATETIME = f"<{NS_TIME}inXSDDateTime>"

URI_TRAFFIC_OBSERVATION = f"<{NS_TRAFFIC}TrafficObservation>"
URI_TRAFFIC_VEHICLE_COUNT = f"<{NS_TRAFFIC}VehicleCount>"
URI_TRAFFIC_OCCUPANCY_TIME = f"<{NS_TRAFFIC}OccupancyTime>"
URI_TRAFFIC_COVERAGE_COUNT = f"<{NS_TRAFFIC}coverageCount>"
URI_TRAFFIC_COVERAGE_OCC = f"<{NS_TRAFFIC}coverageOccupancy>"
URI_TRAFFIC_IMPUTED_RATE = f"<{NS_TRAFFIC}imputedRate>"
URI_TRAFFIC_CLEAN_OBS_RATE = f"<{NS_TRAFFIC}cleanObservedRate>"

URI_SC_OBSERVED_AT_TIMEINDEX = f"<{NS_SC}observedAtTimeIndex>"
URI_SC_AGG_WINDOW_SECONDS = f"<{NS_SC}aggregationWindowSeconds>"


def u(uri: str) -> str:
    return f"<{uri}>"


def lit_double(x) -> str:
    return f"\"{float(x)}\"^^<{NS_XSD}double>"


def lit_long(x) -> str:
    return f"\"{int(x)}\"^^<{NS_XSD}long>"


def lit_int(x) -> str:
    return f"\"{int(x)}\"^^<{NS_XSD}integer>"


def lit_datetime(x: str) -> str:
    return f"\"{x}\"^^<{NS_XSD}dateTime>"


def triple(s: str, p: str, o: str) -> str:
    return f"{s} {p} {o} .\n"


def safe_local(text: str) -> str:
    return urllib.parse.quote(str(text).strip(), safe="")


def normalize_ts(ts):
    if ts is None:
        return None

    if isinstance(ts, dt.datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=dt.timezone.utc)
        return ts.astimezone(dt.timezone.utc)

    ts2 = dt.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    if ts2.tzinfo is None:
        ts2 = ts2.replace(tzinfo=dt.timezone.utc)
    return ts2.astimezone(dt.timezone.utc)


def freq_to_seconds(freq_value) -> int:
    if freq_value is None:
        return 600

    freq = str(freq_value).strip().lower()

    if freq.endswith("min"):
        return int(freq.replace("min", "")) * 60

    if freq.endswith("h"):
        return int(freq.replace("h", "")) * 3600

    return 600


def is_valid_number(x) -> bool:
    return x is not None and not (isinstance(x, float) and math.isnan(x))


def build_traffic_abox(
    input_parquet: str | Path,
    output_nt_gz: str | Path,
    sensor_map_json: str | Path,
    sensor_to_lane_json: str | Path,
    batch_size: int = 100_000,
    threads: int = 8,
) -> Path:
    input_parquet = Path(input_parquet)
    output_nt_gz = Path(output_nt_gz)
    sensor_map_json = Path(sensor_map_json)
    sensor_to_lane_json = Path(sensor_to_lane_json)

    if not input_parquet.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_parquet}")

    output_nt_gz.parent.mkdir(parents=True, exist_ok=True)

    with open(sensor_map_json, "r", encoding="utf-8") as f:
        sensor_uri_map = json.load(f)

    try:
        with open(sensor_to_lane_json, "r", encoding="utf-8") as f:
            sensor_to_lane_map = json.load(f)
    except Exception:
        sensor_to_lane_map = {}

    print("Loaded maps:")
    print(f"  sensors: {len(sensor_uri_map)}")
    print(f"  sensor→lane: {len(sensor_to_lane_map)}")

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={threads};")
    con.execute("SET preserve_insertion_order=false;")

    input_sql = input_parquet.as_posix()

    total_obs = 0
    total_time_instants = 0

    with gzip.open(output_nt_gz, "wt", encoding="utf-8") as fout:
        q_time = f"""
        SELECT DISTINCT timestamp
        FROM read_parquet('{input_sql}')
        WHERE timestamp IS NOT NULL
        """

        cur_time = con.execute(q_time)

        while True:
            rows = cur_time.fetchmany(batch_size)
            if not rows:
                break

            buf = []

            for (ts,) in rows:
                ts = normalize_ts(ts)
                if ts is None:
                    continue

                t_idx = int(ts.timestamp())
                iso_t = ts.isoformat()
                t_inst = u(f"{NS_SCTIME}t_{t_idx}")

                buf.append(triple(t_inst, URI_RDF_TYPE, URI_TIME_INSTANT))
                buf.append(triple(t_inst, URI_TIME_IN_XSD_DATETIME, lit_datetime(iso_t)))

                total_time_instants += 1

            fout.write("".join(buf))
            print(f"[time] written instants so far: {total_time_instants}")

        q_obs = f"""
        SELECT
            sensor_id,
            timestamp,
            count_agg,
            occupancy_time_agg,
            coverage_count,
            coverage_dwell,
            imputed_rate,
            is_clean_observed_rate,
            freq
        FROM read_parquet('{input_sql}')
        WHERE sensor_id IS NOT NULL
          AND timestamp IS NOT NULL
          AND (count_agg IS NOT NULL OR occupancy_time_agg IS NOT NULL)
        """

        cur_obs = con.execute(q_obs)

        while True:
            rows = cur_obs.fetchmany(batch_size)
            if not rows:
                break

            buf = []

            for (
                sid,
                ts,
                count_agg,
                occ_time,
                cov_c,
                cov_o,
                imputed_rate,
                obs_rate,
                freq,
            ) in rows:
                sid = str(sid).strip()

                sensor_uri_str = sensor_uri_map.get(sid)
                if not sensor_uri_str:
                    continue

                sensor_uri = u(sensor_uri_str)

                lane_uri_str = sensor_to_lane_map.get(sid)
                lane_uri = u(lane_uri_str) if lane_uri_str else None

                ts = normalize_ts(ts)
                if ts is None:
                    continue

                t_idx = int(ts.timestamp())
                t_inst = u(f"{NS_SCTIME}t_{t_idx}")
                sid_safe = safe_local(sid)
                window_sec = freq_to_seconds(freq)

                if is_valid_number(count_agg):
                    obs_count = u(f"{NS_EX}obsCount_{sid_safe}_{t_idx}")

                    buf.append(triple(obs_count, URI_RDF_TYPE, URI_SOSA_OBSERVATION))
                    buf.append(triple(obs_count, URI_RDF_TYPE, URI_TRAFFIC_OBSERVATION))
                    buf.append(triple(obs_count, URI_SOSA_MADE_BY_SENSOR, sensor_uri))
                    buf.append(triple(obs_count, URI_SOSA_OBSERVED_PROPERTY, URI_TRAFFIC_VEHICLE_COUNT))
                    buf.append(triple(obs_count, URI_SOSA_HAS_SIMPLE_RESULT, lit_double(count_agg)))
                    buf.append(triple(obs_count, URI_SOSA_PHENOMENON_TIME, t_inst))
                    buf.append(triple(obs_count, URI_SC_OBSERVED_AT_TIMEINDEX, lit_long(t_idx)))
                    buf.append(triple(obs_count, URI_SC_AGG_WINDOW_SECONDS, lit_int(window_sec)))

                    if lane_uri:
                        buf.append(triple(obs_count, URI_SOSA_HAS_FEATURE_INTEREST, lane_uri))
                    if is_valid_number(cov_c):
                        buf.append(triple(obs_count, URI_TRAFFIC_COVERAGE_COUNT, lit_double(cov_c)))
                    if is_valid_number(imputed_rate):
                        buf.append(triple(obs_count, URI_TRAFFIC_IMPUTED_RATE, lit_double(imputed_rate)))
                    if is_valid_number(obs_rate):
                        buf.append(triple(obs_count, URI_TRAFFIC_CLEAN_OBS_RATE, lit_double(obs_rate)))

                    total_obs += 1

                if is_valid_number(occ_time):
                    obs_occ = u(f"{NS_EX}obsOcc_{sid_safe}_{t_idx}")

                    buf.append(triple(obs_occ, URI_RDF_TYPE, URI_SOSA_OBSERVATION))
                    buf.append(triple(obs_occ, URI_RDF_TYPE, URI_TRAFFIC_OBSERVATION))
                    buf.append(triple(obs_occ, URI_SOSA_MADE_BY_SENSOR, sensor_uri))
                    buf.append(triple(obs_occ, URI_SOSA_OBSERVED_PROPERTY, URI_TRAFFIC_OCCUPANCY_TIME))
                    buf.append(triple(obs_occ, URI_SOSA_HAS_SIMPLE_RESULT, lit_double(occ_time)))
                    buf.append(triple(obs_occ, URI_SOSA_PHENOMENON_TIME, t_inst))
                    buf.append(triple(obs_occ, URI_SC_OBSERVED_AT_TIMEINDEX, lit_long(t_idx)))
                    buf.append(triple(obs_occ, URI_SC_AGG_WINDOW_SECONDS, lit_int(window_sec)))

                    if lane_uri:
                        buf.append(triple(obs_occ, URI_SOSA_HAS_FEATURE_INTEREST, lane_uri))
                    if is_valid_number(cov_o):
                        buf.append(triple(obs_occ, URI_TRAFFIC_COVERAGE_OCC, lit_double(cov_o)))
                    if is_valid_number(imputed_rate):
                        buf.append(triple(obs_occ, URI_TRAFFIC_IMPUTED_RATE, lit_double(imputed_rate)))
                    if is_valid_number(obs_rate):
                        buf.append(triple(obs_occ, URI_TRAFFIC_CLEAN_OBS_RATE, lit_double(obs_rate)))

                    total_obs += 1

            fout.write("".join(buf))
            print(f"[obs] written observations so far: {total_obs}")

    con.close()

    print("Traffic ABox finished.")
    print(f"Output: {output_nt_gz}")
    print(f"Total time instants: {total_time_instants}")
    print(f"Total observations: {total_obs}")

    return output_nt_gz
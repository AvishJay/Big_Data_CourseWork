
import csv
import sys
import time
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException


URL    = "http://localhost:8086"
TOKEN  = "bigdata-supersecret-token"
ORG    = "default"
BUCKET = "weather_raw"


FIELD_COLUMNS = {
    "Temperature (C)": "temperature_c",
    "Apparent Temperature (C)": "apparent_temperature_c",
    "Humidity": "humidity",
    "Wind Speed (km/h)": "wind_speed_kmh",
    "Wind Bearing (degrees)": "wind_bearing_degrees",
    "Visibility (km)": "visibility_km",
    "Pressure (millibars)": "pressure_millibars",
}


def to_float(v):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def build_points(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        print(f"[detect] columns: {reader.fieldnames}")
        for row in reader:
            # e.g. "2006-04-01 00:00:00.000 +0200" - offset varies with DST
            ts = datetime.strptime(row["Formatted Date"], "%Y-%m-%d %H:%M:%S.%f %z")

            precip = (row.get("Precip Type") or "").strip().lower()
            if precip in ("", "null"):
                precip = "none"
            summary = (row.get("Summary") or "").strip() or "unknown"

            p = (
                Point("weather")
                .tag("precip_type", precip)
                .tag("summary", summary)
                .time(ts, WritePrecision.S)
            )
            has_field = False
            for src, field in FIELD_COLUMNS.items():
                val = to_float(row.get(src))
                if val is not None:
                    p = p.field(field, val)
                    has_field = True
            if has_field:
                yield p


def write_with_retry(write_api, batch, attempts=5):
    """The CSV is chronologically ordered, so an early batch can span months
    of brand-new shard ranges; InfluxDB occasionally needs a couple of tries
    to finish creating those shards before a write against them succeeds."""
    for attempt in range(1, attempts + 1):
        try:
            write_api.write(bucket=BUCKET, record=batch)
            return
        except ApiException as e:
            if attempt == attempts:
                raise
            wait = 2 * attempt
            print(f"[retry] write failed ({e.status}), retrying in {wait}s "
                  f"(attempt {attempt}/{attempts})...")
            time.sleep(wait)


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: python ingest.py <path-to-weatherHistory.csv>")

    with InfluxDBClient(url=URL, token=TOKEN, org=ORG, timeout=60_000) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        batch, total = [], 0
        for point in build_points(sys.argv[1]):
            batch.append(point)
            if len(batch) >= 1000:
                write_with_retry(write_api, batch)
                total += len(batch)
                print(f"[write] {total} points...")
                batch = []
        if batch:
            write_with_retry(write_api, batch)
            total += len(batch)
        print(f"[done] {total} points written to bucket '{BUCKET}' "
              f"with historical timestamps (2006-2016, hourly resolution).")


if __name__ == "__main__":
    main()

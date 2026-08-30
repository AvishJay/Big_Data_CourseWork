import "math"

data = from(bucket: "weather_raw")
  |> range(start: 2000-01-01T00:00:00Z, stop: now())
  |> filter(fn: (r) => r._measurement == "weather")
  |> filter(fn: (r) => r._field == "temperature_c")
  |> group(columns: ["_field"])

mu = data |> mean()   |> findRecord(fn: (key) => true, idx: 0)
sd = data |> stddev() |> findRecord(fn: (key) => true, idx: 0)

data
  |> filter(fn: (r) => math.abs(x: r._value - mu._value) > 2.0 * sd._value)
  |> yield(name: "anomalies_beyond_2_sigma")



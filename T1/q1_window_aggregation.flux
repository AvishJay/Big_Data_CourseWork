from(bucket: "weather_raw")
  |> range(start: 2000-01-01T00:00:00Z, stop: now())
  |> filter(fn: (r) => r._measurement == "weather")
  |> filter(fn: (r) => r._field == "temperature_c")
  |> group(columns: ["_field"])
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> yield(name: "hourly_mean_temperature")



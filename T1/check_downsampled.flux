from(bucket: "weather_downsampled")
  |> range(start: 2000-01-01T00:00:00Z, stop: now())
  |> limit(n: 10)

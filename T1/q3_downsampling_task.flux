option task = {name: "downsample_weather", every: 1h}

from(bucket: "weather_raw")
  |> range(start: -task.every)
  |> filter(fn: (r) => r._measurement == "weather")
  |> group(columns: ["_measurement", "_field"])
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> set(key: "rollup", value: "hourly_mean")
  |> to(bucket: "weather_downsampled", org: "default")


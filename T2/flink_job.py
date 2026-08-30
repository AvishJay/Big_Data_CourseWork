
import json

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

KAFKA_BOOTSTRAP = "kafka:19092"          
TOPIC = "traffic-telemetry"


class EventTimeAssigner(TimestampAssigner):
    """Use the ORIGINAL event timestamp carried in the message payload."""
    def extract_timestamp(self, value, record_timestamp):
        return value[2]                  # event_ts_ms


class WindowTotal(ProcessWindowFunction):
    """Emit one line per (sensor, window) with the aggregated vehicle total."""
    def process(self, key, context, elements):
        total = sum(e[1] for e in elements)
        w = context.window()
        yield (f"sensor={key}  window=[{w.start} .. {w.end}]  "
               f"vehicle_total={total}")


def parse(raw):
    d = json.loads(raw)
    return d.get("sensor_id", "unknown"), int(d.get("volume", 0)), int(d["event_ts_ms"])


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)               

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id("flink-traffic-analytics")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermarks = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(EventTimeAssigner())
        .with_idleness(Duration.of_seconds(30))
    )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-traffic-telemetry")
        .map(parse, output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]))
        .assign_timestamps_and_watermarks(watermarks)
        .key_by(lambda t: t[0], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(10)))
        .process(WindowTotal(), output_type=Types.STRING())
        .print()
    )

    env.execute("traffic-telemetry-10min-tumbling-totals")


if __name__ == "__main__":
    main()

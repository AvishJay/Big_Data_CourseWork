
import argparse
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast

DATA_PATH = "/opt/data/web-BerkStan.txt"
OUT_DIR = "/opt/data/output"


def main(hold: bool):
    spark = (
        SparkSession.builder
        .appName("BerkStan-InDegree-Analytics")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    
    raw = spark.read.text(DATA_PATH)                       
    edges = (
        raw.filter(~F.col("value").startswith("#"))        
           .select(F.split(F.col("value"), r"\s+").alias("p"))
           .select(
               F.col("p")[0].cast("long").alias("src"),
               F.col("p")[1].cast("long").alias("dst"),
           )
           .where(F.col("src").isNotNull() & F.col("dst").isNotNull())
    )

    edges.cache()
    n_edges = edges.count()                               
    print(f"[metrics] parsed edges: {n_edges:,}  (+{time.time()-t0:.1f}s)")

  
    in_deg = edges.groupBy("dst").agg(F.count("*").alias("in_degree"))

    degree_dist = (
        in_deg.groupBy("in_degree")
              .agg(F.count("*").alias("num_nodes"))
              .orderBy("in_degree")
    )
    print("[result] in-degree distribution (first 20 rows):")
    degree_dist.show(20)

    top50 = in_deg.orderBy(F.desc("in_degree")).limit(50)
    print("[result] Top 50 destination nodes by in-degree:")
    top50.show(50, truncate=False)

    from pyspark.sql.window import Window
    ranked = top50.withColumn(
        "rank", F.row_number().over(Window.orderBy(F.desc("in_degree")))
    )
    hub_edges = edges.join(broadcast(ranked), edges.dst == ranked.dst, "inner")
    share = hub_edges.count() / n_edges * 100
    print(f"[result] {share:.2f}% of all edges point at the top-50 hub nodes "
          f"(computed via broadcast join - inspect the SQL tab: "
          f"BroadcastHashJoin, no shuffle on the large side)")

    degree_dist.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT_DIR}/in_degree_distribution")
    ranked.coalesce(1).write.mode("overwrite").option("header", True) \
        .csv(f"{OUT_DIR}/top50_destinations")

    print(f"[metrics] total wall-clock: {time.time()-t0:.1f}s")
    print(f"[metrics] output written to {OUT_DIR}/ on the shared volume")

    if hold:
        input("\n>>> Application UI held open at http://localhost:4040 - "
              "take your screenshots, then press Enter to finish. ")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hold", action="store_true",
                    help="pause at the end so the :4040 UI stays up for metric capture")
    args = ap.parse_args()
    main(args.hold)

# Databricks notebook source
# MAGIC %md
# MAGIC This section initializes the Spark environment. We configure memory settings and create a Spark session, which is the entry point for all Spark operations. This ensures our cluster can handle the datasets efficiently.

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Data Validation Loads - 
# MAGIC Here we load the raw parquet files from the S3 bucket for each entity (customers, artists, dates, sessions, subscriptions, tracks). This step verifies that data was properly uploaded and accessible before analysis.

# COMMAND ----------

# Create Spark session
spark = SparkSession.builder \
    .appName("Validation") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Load the customer table from S3
validation=spark.read.parquet("s3://beat-blast-1753142400/customers/customers.parquet")

# Display the data
display(validation)

# COMMAND ----------

# Load the artists table from S3
validation=spark.read.parquet("s3://beat-blast-1753142400/artists/artists.parquet")

# Display the data
display(validation)

# COMMAND ----------

# Load the dates table from S3
validation=spark.read.parquet("s3://beat-blast-1753142400/dates/date_dimension.parquet")

# Display the data
display(validation)

# COMMAND ----------

# Load the sessions table from S3
validation=spark.read.parquet("s3://beat-blast-1753142400/sessions/2025/08/sessions.parquet")

# Display the data
display(validation)

# COMMAND ----------

# Load the subscriptions table from S3
validation=spark.read.parquet("s3://beat-blast-1753142400/subscriptions/subscriptions.parquet")

# Display the data
display(validation)

# COMMAND ----------

# Load the tracks table from S3
validation=spark.read.parquet("s3://beat-blast-1753142400/tracks/tracks.parquet")

# Display the data
display(validation)

# COMMAND ----------

from pyspark.sql import functions as F, types as T


# COMMAND ----------

# MAGIC %md
# MAGIC KPI

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Define Tables for KPI - 
# MAGIC We load the key datasets used in KPI calculations. Recursive lookup ensures all partitioned data (organized by folders such as date) is included. We also print the schema to confirm column availability for later transformations.

# COMMAND ----------

from pyspark.sql import functions as F

# Customers
customers = (spark.read
             .option("recursiveFileLookup","true")
             .parquet("s3://beat-blast-1753142400/customers/"))

# Sessions (often partitioned by date → read recursively)
sessions = (spark.read
            .option("recursiveFileLookup","true")
            .parquet("s3://beat-blast-1753142400/sessions/"))

# Subscriptions
subscriptions = (spark.read
                 .option("recursiveFileLookup","true")
                 .parquet("s3://beat-blast-1753142400/subscriptions/"))

customers.printSchema()
sessions.printSchema()
subscriptions.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Session Normalization - Sessions often come with timestamps in different formats (seconds, milliseconds, strings). Here, we normalize the session_start field into a proper timestamp column (session_start_ts) to ensure consistency across KPIs.

# COMMAND ----------

# Detect a usable session-start column and convert to Spark timestamp.
CANDIDATES = {"session_start","start_time","started_at","session_ts","ts","timestamp","start_timestamp"}
lower_to_orig = {c.lower(): c for c in sessions.columns}
start_col = next((lower_to_orig[c] for c in CANDIDATES if c in lower_to_orig), None)
if start_col is None:
    raise ValueError(f"No session start column found. Available: {sessions.columns}")

# Convert seconds/ms/us/ns epochs or strings to proper timestamp
expr = (
    F.when(F.col(start_col) > F.lit(1_000_000_000_000_000_000), F.col(start_col) / F.lit(1_000_000_000))  # ns→s
     .when(F.col(start_col) > F.lit(1_000_000_000_000),        F.col(start_col) / F.lit(1_000))           # ms→s
     .otherwise(F.col(start_col).cast("double"))                                                    # seconds or castable
)

# If numeric → from_unixtime; else parse string
sess = sessions.withColumn(
    "session_start_ts",
    F.when(F.col(start_col).cast("string").rlike("^[0-9]+$"), F.to_timestamp(F.from_unixtime(expr)))
     .otherwise(F.to_timestamp(F.col(start_col)))
)

# sanity
sess.select(start_col, "session_start_ts").orderBy(F.col(start_col)).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 5. KPI # 1 - Session Adoption Rate
# MAGIC This KPI measures the percentage of total customers who started at least one session in the analysis window (July–August 2025). It provides insight into overall platform adoption.

# COMMAND ----------

WINDOW_START = "2025-07-01"
WINDOW_END   = "2025-08-31"

active_customers = (sess
    .filter((F.to_date("session_start_ts") >= F.lit(WINDOW_START)) &
            (F.to_date("session_start_ts") <= F.lit(WINDOW_END)))
    .select("customer_id").distinct())

total_customers = customers.select("customer_id").distinct()

kpi1 = (active_customers.agg(F.count("*").alias("customers_with_sessions"))
        .crossJoin(total_customers.agg(F.count("*").alias("all_customers")))
        .withColumn("session_adoption_rate_pct",
                    F.round(F.col("customers_with_sessions")*100.0/F.col("all_customers"), 2)))

display(kpi1)

# COMMAND ----------

# MAGIC %md
# MAGIC 6. KPI # 2 – Engaged Users (% with ≥ 80 sessions in window)
# MAGIC This KPI identifies the proportion of active users who are highly engaged (≥80 sessions within the analysis window). It highlights power users and usage intensity.

# COMMAND ----------

# uses `sess` from KPI #1 (has session_start_ts already)
ENGAGED_MIN_SESSIONS = 80
WINDOW_START = "2025-07-01"
WINDOW_END   = "2025-08-31"

sessions_win = (sess
    .filter((F.to_date("session_start_ts") >= F.lit(WINDOW_START)) &
            (F.to_date("session_start_ts") <= F.lit(WINDOW_END))))

per_user = (sessions_win
    .groupBy("customer_id")
    .agg(F.countDistinct("session_id").alias("sessions_in_window")))

engaged = per_user.filter(F.col("sessions_in_window") >= ENGAGED_MIN_SESSIONS)
active  = per_user  # all users who had ≥1 session in window

kpi2 = (engaged.agg(F.count("*").alias("engaged_users"))
        .crossJoin(active.agg(F.count("*").alias("active_users")))
        .withColumn("engaged_users_pct",
                    F.round(F.col("engaged_users")*100.0/F.col("active_users"), 2)))

display(kpi2)

# COMMAND ----------

per_user.orderBy("sessions_in_window", ascending=False).show(20)



(per_user
   .withColumn("bucket",
               F.when(F.col("sessions_in_window") >= 3, "3+ sessions")
                .when(F.col("sessions_in_window") == 2, "2 sessions")
                .otherwise("1 session"))
   .groupBy("bucket")
   .count()
   .show())


# COMMAND ----------

# MAGIC %md
# MAGIC 7. KPI 3 - Subscription Adoption (paid share)
# MAGIC This KPI tracks how many active users converted to paid subscriptions during the analysis window. It uses subscription start/end dates to detect overlap with the window and filters for paid/active types.
# MAGIC
# MAGIC paid_share_pct = customers with an active paid subscription in the window ÷ active customers in the window

# COMMAND ----------

# Window
WINDOW_START = "2025-07-01"
WINDOW_END   = "2025-08-31"

# Active customers in window (from sessions already normalized as `sess`)
sessions_win = (sess
    .filter((F.to_date("session_start_ts") >= F.lit(WINDOW_START)) &
            (F.to_date("session_start_ts") <= F.lit(WINDOW_END))))
active_customers = sessions_win.select("customer_id").distinct()

# Load subscriptions and normalize timestamps
subs = (spark.read
        .option("recursiveFileLookup","true")
        .parquet("s3://beat-blast-1753142400/subscriptions/"))


subs_ts = (subs
    .withColumn("sub_start_ts", F.to_timestamp(F.col("effective_start_date")))
    .withColumn("sub_end_ts",   F.to_timestamp(F.col("effective_end_date"))))

# Window overlap: subscription active at any point in the analysis window
win_start_ts = F.to_timestamp(F.lit(WINDOW_START))
win_end_ts   = F.to_timestamp(F.lit(WINDOW_END))
overlaps = (
    (F.col("sub_start_ts") <= win_end_ts) &
    (F.coalesce(F.col("sub_end_ts"), win_end_ts) >= win_start_ts)
)

# Paid definition: prefer subscription_type; fallback to amount_paid > 0
paid_cond = F.coalesce(
    (~F.lower(F.col("subscription_type")).like("%free%")).cast("boolean"),
    (F.col("amount_paid").cast("double") > 0)
)

# Status (optional): treat 'active' or 'trialing' as active; if no status, keep all overlaps
status_active = F.when(
    F.col("status").isNotNull(),
    F.lower(F.col("status")).isin("active","trialing")
).otherwise(F.lit(True))

subs_active_paid = (subs_ts
    .where(overlaps & status_active & paid_cond)
    .select("customer_id").distinct())

# KPI: paid share among active customers
kpi3 = (subs_active_paid.agg(F.count("*").alias("paid_active_customers"))
        .crossJoin(active_customers.agg(F.count("*").alias("active_customers")))
        .withColumn("paid_share_pct",
                    F.round(F.col("paid_active_customers")*100.0/F.col("active_customers"), 2)))

display(kpi3)

# COMMAND ----------

# MAGIC %md
# MAGIC 8. We compile all KPI values into a single summary table for presentation. This makes it easier for business stakeholders to review results at a glance.

# COMMAND ----------

from pyspark.sql import Row

# pull values out of each KPI DF
k1 = kpi1.first().asDict()
k2 = kpi2.first().asDict()
k3 = kpi3.first().asDict()

summary_rows = [
    Row(metric="Session Adoption Rate (%)", value=k1["session_adoption_rate_pct"]),
    Row(metric=f"Engaged Users ≥{ENGAGED_MIN_SESSIONS} Sessions (%)", value=k2["engaged_users_pct"]),
    Row(metric="Paid Share (%)", value=k3["paid_share_pct"])
]

summary_df = spark.createDataFrame(summary_rows)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Use Case**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 9. streaming – step 1 (start the 5‑minute session counter)
# MAGIC Here we simulate a real-time streaming pipeline using Databricks Auto Loader. Session events are ingested continuously, converted into timestamps, and aggregated into 5-minute windows.

# COMMAND ----------

# Paths
INPUT_PATH      = "s3://beat-blast-1753142400/sessions/2025/08/"
SCHEMA_PATH     = "s3://beat-blast-1753142400/_schema/stream_sessions_5m/"
CHECKPOINT_PATH = "s3://beat-blast-1753142400/chk/stream_sessions_5m/"
OUTPUT_PATH     = "s3://beat-blast-1753142400/gold/stream_sessions_5m/"

# 1) Read raw events as stream (Auto Loader)
stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .load(INPUT_PATH))

# 2) Normalize timestamps
sessions_stream = stream.withColumn(
    "session_start_ts",
    F.to_timestamp(F.col("start_timestamp"))
)

# 3) Aggregate into 5-minute windows
agg_5m = (sessions_stream
    .withWatermark("session_start_ts", "1 hour")
    .groupBy(F.window("session_start_ts", "5 minutes").alias("win"))
    .agg(
        F.count("*").alias("sessions"),
        F.approx_count_distinct("customer_id").alias("customers")
    )
    .select(
        F.col("win").start.alias("window_start"),
        F.col("win").end.alias("window_end"),
        "sessions","customers"
    ))

# 4) Write once into Delta output
q = (agg_5m.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(once=True)   # run once then stop
    .start(OUTPUT_PATH))

q.awaitTermination()

# COMMAND ----------

dbutils.fs.ls(INPUT_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Streaming - step 2 (read & display the 5-minute table)
# MAGIC We store the streaming aggregates into Delta format, then reload them for visualization. Metrics include total sessions, distinct customers, and sessions per minute.

# COMMAND ----------

from pyspark.sql import functions as F

df_5m = spark.read.format("delta").load(OUTPUT_PATH)
display(
  df_5m.orderBy("window_start")
       .withColumn("sessions_per_min", F.round(F.col("sessions")/5.0, 2))
)

# COMMAND ----------

# Read the Delta output and add per-minute rate
from pyspark.sql import functions as F

df_5m = spark.read.format("delta").load(OUTPUT_PATH)

display(
  df_5m.orderBy("window_start")
       .withColumn("sessions_per_min", F.round(F.col("sessions")/5.0, 2))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 11. Streaming Alert Logic - 
# MAGIC We implement a simple anomaly detection: if the current session count exceeds the 95th percentile of recent activity, we flag a spike. This is useful for detecting unusual traffic surges.

# COMMAND ----------

# Quick "current vs. baseline" check
from pyspark.sql import functions as F

# last 12 windows (~1 hour) as baseline
recent = (df_5m.orderBy(F.desc("window_start")).limit(12)
          .agg(F.avg("sessions").alias("recent_avg"),
               F.expr("percentile(sessions, 0.95)").alias("recent_p95")))

current = df_5m.orderBy(F.desc("window_start")).limit(1)

alert = (current.crossJoin(recent)
         .select("window_start","window_end","sessions","customers",
                 "recent_avg","recent_p95",
                 (F.col("sessions") > F.col("recent_p95")).alias("spike_flag")))

display(alert)

# COMMAND ----------

# MAGIC %md
# MAGIC 12. Machine Learning Demo

# COMMAND ----------

# 12) Machine Learning — Threshold Classifier

from pyspark.sql import functions as F

ENGAGED_MIN_SESSIONS = 80
WINDOW_START = "2025-07-01"
WINDOW_END   = "2025-08-31"

# 1) Labeled dataset
sessions_win = (
    sess
    .filter(
        (F.to_date("session_start_ts") >= F.lit(WINDOW_START)) &
        (F.to_date("session_start_ts") <= F.lit(WINDOW_END))
    )
    .groupBy("customer_id")
    .agg(F.countDistinct("session_id").alias("sessions_in_window"))
)

data = (
    sessions_win
    .withColumn("label", F.when(F.col("sessions_in_window") >= ENGAGED_MIN_SESSIONS, F.lit(1)).otherwise(F.lit(0)))
    .select("customer_id","sessions_in_window","label")
    .withColumn("label", F.col("label").cast("int"))
)

# 2) Train / Test split
train, test = data.randomSplit([0.8, 0.2], seed=42)

# 3) Candidate thresholds from train quantiles
quantiles = [i/20.0 for i in range(1,20)]  # 5%, 10%, ..., 95%
qs = [float(q) for q in train.approxQuantile("sessions_in_window", quantiles, 0.01) if q is not None]

def eval_with_threshold(df, thr):
    pred = (df
        .withColumn("prediction", F.when(F.col("sessions_in_window") >= F.lit(thr), 1).otherwise(0))
        .withColumn("prediction", F.col("prediction").cast("int"))
    )
    agg = (pred.agg(
        F.sum(F.when((F.col("label")==1) & (F.col("prediction")==1), 1).otherwise(0)).alias("TP"),
        F.sum(F.when((F.col("label")==0) & (F.col("prediction")==1), 1).otherwise(0)).alias("FP"),
        F.sum(F.when((F.col("label")==0) & (F.col("prediction")==0), 1).otherwise(0)).alias("TN"),
        F.sum(F.when((F.col("label")==1) & (F.col("prediction")==0), 1).otherwise(0)).alias("FN"),
    )).collect()[0].asDict()
    TP, FP, TN, FN = agg["TP"], agg["FP"], agg["TN"], agg["FN"]
    precision = TP / (TP+FP) if (TP+FP)>0 else 0.0
    recall    = TP / (TP+FN) if (TP+FN)>0 else 0.0
    f1        = (2*precision*recall)/(precision+recall) if (precision+recall)>0 else 0.0
    accuracy  = (TP+TN) / (TP+FP+TN+FN) if (TP+FP+TN+FN)>0 else 0.0
    return {"threshold": float(thr), "precision": precision, "recall": recall,
            "f1": f1, "accuracy": accuracy, "TP": TP, "FP": FP, "TN": TN, "FN": FN}

# 4) Pick best threshold on TRAIN by F1
train_metrics = [eval_with_threshold(train, t) for t in qs] if qs else [{"threshold": float(ENGAGED_MIN_SESSIONS), "f1":0}]
best = max(train_metrics, key=lambda x: x["f1"])
best_threshold = float(best["threshold"])
print("Best threshold picked on TRAIN =", best_threshold)

# 5) Evaluate on TEST
test_pred = (test
    .withColumn("prediction", F.when(F.col("sessions_in_window") >= F.lit(best_threshold), 1).otherwise(0))
    .withColumn("prediction", F.col("prediction").cast("int"))
)

conf = (test_pred.agg(
    F.sum(F.when((F.col("label")==1) & (F.col("prediction")==1), 1).otherwise(0)).alias("TP"),
    F.sum(F.when((F.col("label")==0) & (F.col("prediction")==1), 1).otherwise(0)).alias("FP"),
    F.sum(F.when((F.col("label")==0) & (F.col("prediction")==0), 1).otherwise(0)).alias("TN"),
    F.sum(F.when((F.col("label")==1) & (F.col("prediction")==0), 1).otherwise(0)).alias("FN"),
))
display(conf)

r = conf.collect()[0].asDict()
TP, FP, TN, FN = [int(r[k]) for k in ("TP","FP","TN","FN")]
precision = TP / (TP+FP) if (TP+FP)>0 else 0.0
recall    = TP / (TP+FN) if (TP+FN)>0 else 0.0
f1        = (2*precision*recall)/(precision+recall) if (precision+recall)>0 else 0.0
accuracy  = (TP+TN) / (TP+FP+TN+FN) if (TP+FP+TN+FN)>0 else 0.0

print(f"TEST metrics @ threshold={best_threshold:.2f}  |  "
      f"accuracy={accuracy:.3f}  precision={precision:.3f}  recall={recall:.3f}  f1={f1:.3f}")

# 6) Preview scored rows (probability-like score for display)
max_sess = test.agg(F.max("sessions_in_window")).first()[0] or 1
scored = test_pred.withColumn("probability_like", F.col("sessions_in_window")/F.lit(max_sess))
display(scored.select("customer_id","sessions_in_window","label","prediction","probability_like")
        .orderBy(F.desc("probability_like")))


# COMMAND ----------

# Save KPI tables
kpi1.write.format("delta").mode("overwrite").saveAsTable("default.kpi1")
kpi2.write.format("delta").mode("overwrite").saveAsTable("default.kpi2")
kpi3.write.format("delta").mode("overwrite").saveAsTable("default.kpi3")

# Save KPI summary
summary_df.write.format("delta").mode("overwrite").saveAsTable("default.kpi_summary")

# Save streaming 5-minute aggregates
df_5m.write.format("delta").mode("overwrite").saveAsTable("default.sessions_5m")

# Save streaming alert results
alert.write.format("delta").mode("overwrite").saveAsTable("default.sessions_alerts")
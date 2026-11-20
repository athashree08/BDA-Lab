# Experiment 10 – Real-Time Traffic Monitoring and Data Storytelling

## Aim

To demonstrate how a streaming pipeline can process traffic-sensor events using Kafka and Spark Structured Streaming, calculate time-window traffic metrics, and communicate congestion insights through a Power BI dashboard.

## 1. Prerequisites

Required software:
- Java 17 or later
- Apache Kafka
- Apache Spark
- PySpark
- Python
- Power BI Desktop (Windows)

Check installations:

```bash
java -version
python --version
pyspark --version
```

Set the Kafka installation directory:

```bash
export KAFKA_HOME="$HOME/kafka_2.13-4.3.1"
```

Activate the Python environment:

```bash
source ~/pyspark-env/bin/activate
```

## 2. Start the Kafka Broker

If Kafka KRaft storage has not been initialized, initialize it once:

```bash
cd "$KAFKA_HOME"

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties
```

Start the Kafka broker:

```bash
bin/kafka-server-start.sh config/server.properties
```

Keep the terminal running.

## 3. Create the Kafka Topic

Open another terminal.

Set the Kafka directory:

```bash
export KAFKA_HOME="$HOME/kafka_2.13-4.3.1"
```

Create a topic named `traffic-events`:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --create \
  --topic traffic-events \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

List the available topics:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --list \
  --bootstrap-server localhost:9092
```

Describe the topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --describe \
  --topic traffic-events \
  --bootstrap-server localhost:9092
```

## 4. Create the Project Directory

```bash
mkdir -p ~/traffic_streaming
cd ~/traffic_streaming
```

## 5. Publish Traffic Events Using Kafka Console Producer

Start the Kafka console producer:

```bash
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --topic traffic-events \
  --bootstrap-server localhost:9092
```

Enter JSON events in the following format:

```json
{"sensor_id":"S101","location":"University Road","timestamp":"2026-11-20T09:00:05","vehicle_count":52,"avg_speed_kmh":10.5}
{"sensor_id":"S102","location":"JM Road","timestamp":"2026-11-20T09:00:15","vehicle_count":30,"avg_speed_kmh":30.0}
{"sensor_id":"S103","location":"University Road","timestamp":"2026-11-20T09:00:25","vehicle_count":48,"avg_speed_kmh":12.0}
{"sensor_id":"S104","location":"JM Road","timestamp":"2026-11-20T09:00:35","vehicle_count":25,"avg_speed_kmh":32.0}
{"sensor_id":"S105","location":"University Road","timestamp":"2026-11-20T09:00:45","vehicle_count":50,"avg_speed_kmh":11.0}
{"sensor_id":"S106","location":"JM Road","timestamp":"2026-11-20T09:00:55","vehicle_count":28,"avg_speed_kmh":31.0}
```

**Important:** Replace the example timestamps with timestamps from the current date and time when running the practical. Events should fall within the same one-minute window for this example.

Press `Ctrl+C` after publishing the events.

## 6. Verify Kafka Events

Read the messages from the Kafka topic:

```bash
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --topic traffic-events \
  --from-beginning \
  --bootstrap-server localhost:9092
```

Verify that the JSON events are received.

Press `Ctrl+C` to stop the consumer.

## 7. Create the Spark Structured Streaming Application

Create the Python file:

```bash
nano ~/traffic_streaming/traffic_stream.py
```

Add the following code:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    avg,
    sum,
    count,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DoubleType
)

import os

# Create Spark session
spark = (
    SparkSession.builder
    .appName("RealTimeTrafficMonitoring")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define incoming JSON schema
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed_kmh", DoubleType(), True)
])

# Read traffic events from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "traffic-events")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Kafka message values into strings
json_stream = kafka_stream.selectExpr(
    "CAST(value AS STRING) AS json_value"
)

# Parse JSON events
traffic_data = (
    json_stream
    .select(
        from_json(col("json_value"), schema).alias("data")
    )
    .select("data.*")
    .filter(
        col("timestamp").isNotNull() &
        col("location").isNotNull()
    )
)

# Group events by one-minute event-time windows and location
traffic_summary = (
    traffic_data
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("location")
    )
    .agg(
        avg("avg_speed_kmh").alias("avg_speed_kmh"),
        sum("vehicle_count").alias("total_vehicles"),
        count("*").alias("event_count")
    )
    .withColumn(
        "congestion_status",
        when(
            col("avg_speed_kmh") < 15,
            "CONGESTED"
        ).otherwise("NORMAL")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("location"),
        col("avg_speed_kmh"),
        col("total_vehicles"),
        col("event_count"),
        col("congestion_status")
    )
)

# Windows-accessible CSV output location
output_file = "/mnt/c/Users/Public/traffic_summary.csv"

# Save each updated aggregate snapshot to CSV
def save_to_csv(batch_df, batch_id):
    pandas_df = batch_df.toPandas()

    pandas_df.to_csv(
        output_file,
        index=False
    )

    print(f"Batch {batch_id} saved to {output_file}")
    pandas_df.show() if hasattr(pandas_df, "show") else print(pandas_df)


# Start streaming query
query = (
    traffic_summary.writeStream
    .outputMode("complete")
    .foreachBatch(save_to_csv)
    .option(
        "checkpointLocation",
        "/tmp/traffic_streaming_checkpoint"
    )
    .start()
)

print("Traffic monitoring stream started...")
print("Listening to Kafka topic: traffic-events")

query.awaitTermination()
```

Save and exit.

## 8. Run the Spark Structured Streaming Application

Check the installed Spark version:

```bash
pyspark --version
```

For PySpark 4.2.0 with Scala 2.13, run:

```bash
cd ~/traffic_streaming

spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  traffic_stream.py
```

Keep the Spark application running.

**Important:** The Kafka connector version and Scala suffix must match the installed Spark version. If your Spark version differs, use its corresponding connector.

## 9. Publish More Traffic Events

Open another terminal:

```bash
export KAFKA_HOME="$HOME/kafka_2.13-4.3.1"
```

Start the producer:

```bash
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --topic traffic-events \
  --bootstrap-server localhost:9092
```

Publish additional traffic events using current timestamps:

```json
{"sensor_id":"S107","location":"University Road","timestamp":"2026-11-20T09:01:05","vehicle_count":60,"avg_speed_kmh":9.0}
{"sensor_id":"S108","location":"JM Road","timestamp":"2026-11-20T09:01:15","vehicle_count":35,"avg_speed_kmh":28.0}
{"sensor_id":"S109","location":"University Road","timestamp":"2026-11-20T09:01:25","vehicle_count":55,"avg_speed_kmh":13.0}
```

The Spark application continuously processes incoming events and updates the aggregate CSV snapshot.

## 10. Verify the Aggregated CSV

Check the output file:

```bash
ls -l /mnt/c/Users/Public/traffic_summary.csv
```

Display the CSV:

```bash
cat /mnt/c/Users/Public/traffic_summary.csv
```

The output contains:

| Column | Description |
|---|---|
| window_start | Start of the one-minute window |
| window_end | End of the one-minute window |
| location | Traffic sensor location |
| avg_speed_kmh | Average reported vehicle speed |
| total_vehicles | Total vehicles reported |
| event_count | Number of sensor events |
| congestion_status | CONGESTED or NORMAL |

The congestion rule is:

```python
avg_speed_kmh < 15
```

## 11. Open the CSV in Power BI

Open Power BI Desktop on Windows.

1. Select **Get Data**.
2. Choose **Text/CSV**.
3. Browse to:

```text
C:\Users\Public\traffic_summary.csv
```

4. Select the CSV file.
5. Click **Load**.
6. Verify that the columns and data types are correct.

## 12. Create the Power BI Dashboard

Create a dashboard titled:

```text
REAL-TIME TRAFFIC MONITORING DASHBOARD
```

Add the following visuals:

### Visual 1 – Average Speed by Location

- Visual: Clustered column chart
- X-axis: `location`
- Y-axis: `avg_speed_kmh`
- Aggregation: Average

### Visual 2 – Average Speed Trend

- Visual: Line chart
- X-axis: `window_start`
- Y-axis: Average of `avg_speed_kmh`
- Legend: `location`

### Visual 3 – Congestion Status

- Visual: Donut chart or stacked bar chart
- Category: `congestion_status`
- Values: Count of `event_count`

### Visual 4 – Traffic Details

- Visual: Table
- Columns:
  - `window_start`
  - `location`
  - `avg_speed_kmh`
  - `total_vehicles`
  - `congestion_status`

### Visual 5 – Total Traffic Events

- Visual: Card
- Value: Sum of `event_count`

### Visual 6 – Total Vehicles Monitored

- Visual: Card
- Value: Sum of `total_vehicles`

## 13. Data Storytelling

Use the dashboard to communicate one concise finding.

Example:

"University Road was flagged as congested in the observed window because its average reported speed was below 15 km/h."

Verify this statement against the actual dashboard output before presenting it.

Answer the following questions using the dashboard:

1. What happened, where, and during which time window?
2. Which visual best supports the claim: location comparison, trend, or detailed table?
3. Are units, time range, threshold, and labels visible?
4. What action could a traffic operator consider, and what additional data would be needed before acting?
5. What are the limitations of this toy dataset and threshold?

## 14. Stop the Applications

Stop the Spark streaming application:

```text
Ctrl + C
```

Stop the Kafka producer:

```text
Ctrl + C
```

Stop the Kafka broker:

```text
Ctrl + C
```

## 15. Cleanup (Optional)

Remove the streaming checkpoint:

```bash
rm -rf /tmp/traffic_streaming_checkpoint
```

Delete the Kafka topic if required:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --delete \
  --topic traffic-events \
  --bootstrap-server localhost:9092
```

Remove the generated CSV if required:

```bash
rm -f /mnt/c/Users/Public/traffic_summary.csv
```

---

## Result

Traffic-sensor events were ingested through Kafka and processed using Spark Structured Streaming to calculate one-minute traffic metrics and identify potential congestion. The aggregate CSV snapshot was visualized in Power BI, and an insight was communicated using data-storytelling principles.

---

**End of Experiment 10**
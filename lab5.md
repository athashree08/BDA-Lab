# Experiment 05 – Machine Learning using Spark MLlib and Kafka

## Aim

Train a classification model using Spark MLlib and apply it to order events arriving through Kafka using Spark Structured Streaming.

## 1. Prerequisites

This practical uses:
- Python
- PySpark
- Apache Kafka (KRaft mode)
- Java 17 or later
- Spark MLlib
- Kafka Structured Streaming connector

Activate the Python environment:

```bash
source ~/pyspark-env/bin/activate
```

Check the installed versions:

```bash
java -version
python --version
pyspark --version
```

Set the Kafka installation directory. Update this path if the installed Kafka directory has a different name.

```bash
export KAFKA_HOME="$HOME/kafka_2.13-4.3.1"
```

## 2. Create the Project Directory

```bash
mkdir -p ~/kafka_spark_ml
cd ~/kafka_spark_ml
```

## 3. Start the Kafka Broker

If Kafka has not been initialized on this machine, format its KRaft storage once:

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
cd "$KAFKA_HOME"

bin/kafka-server-start.sh config/server.properties
```

Keep this terminal running.

## 4. Create the Kafka Topic

Open a second terminal:

```bash
export KAFKA_HOME="$HOME/kafka_2.13-4.3.1"
```

Create a topic named `orders`:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

If the topic already exists, do not create it again.

Verify the topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --list \
  --bootstrap-server localhost:9092
```

Describe the topic:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --describe \
  --topic orders \
  --bootstrap-server localhost:9092
```

## 5. Create the Labelled Training Dataset

Return to the project directory:

```bash
cd ~/kafka_spark_ml
```

Create `order_training.csv`:

```bash
cat > order_training.csv <<'EOF'
amount,items_count,night_order,is_suspicious
1200,1,0,0
2500,2,0,0
3500,1,0,0
4500,2,0,0
5500,3,0,0
7800,2,0,0
9500,1,0,0
12000,3,0,0
18000,4,0,0
22000,4,0,0
28000,5,0,0
32000,4,1,1
40000,3,1,1
45000,4,1,1
55000,5,1,1
65000,1,1,1
75000,2,1,1
85000,5,1,1
95000,4,1,1
120000,6,1,1
EOF
```

Inspect the dataset:

```bash
cat order_training.csv
```

The columns are:
- `amount`: Order amount
- `items_count`: Number of items in the order
- `night_order`: Whether the order was placed at night (1 = Yes, 0 = No)
- `is_suspicious`: Training label (1 = Suspicious, 0 = Normal)

## 6. Train and Save the MLlib Classification Model

Create `train_model.py`:

```bash
cat > train_model.py <<'PY'
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression

spark = (
    SparkSession.builder
    .appName("OrderRiskModelTraining")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Load labelled training data
data = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("order_training.csv")
)

print("Training data:")
data.show()

data.printSchema()

# Convert input columns into a feature vector
assembler = VectorAssembler(
    inputCols=["amount", "items_count", "night_order"],
    outputCol="features"
)

# Create the classification algorithm
classifier = LogisticRegression(
    featuresCol="features",
    labelCol="is_suspicious",
    maxIter=50
)

# Build and train the pipeline
pipeline = Pipeline(stages=[assembler, classifier])

model = pipeline.fit(data)

# Display predictions on training data
predictions = model.transform(data)

print("Training predictions:")
predictions.select(
    "amount",
    "items_count",
    "night_order",
    "is_suspicious",
    "prediction"
).show()

# Save the trained model
model.write().overwrite().save("order_risk_model")

print("Model saved successfully to order_risk_model")

spark.stop()
PY
```

Run the training script:

```bash
python train_model.py
```

Verify that the model directory was created:

```bash
ls order_risk_model
```

## 7. Create the Spark Structured Streaming Consumer

Create `score_stream.py`:

```bash
cat > score_stream.py <<'PY'
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
from pyspark.ml import PipelineModel

spark = (
    SparkSession.builder
    .appName("OrderRiskStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Load the previously trained model
model = PipelineModel.load("order_risk_model")

# Define the incoming JSON event structure
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("items_count", IntegerType(), True),
    StructField("night_order", IntegerType(), True)
])

# Read events from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Kafka message values from binary to strings
json_stream = kafka_stream.selectExpr(
    "CAST(value AS STRING) AS json_value"
)

# Parse JSON messages into columns
orders = (
    json_stream
    .select(from_json(col("json_value"), schema).alias("order"))
    .select("order.*")
)

# Apply the trained model to incoming orders
predictions = model.transform(orders)

# Display order details and classification results
results = predictions.select(
    "order_id",
    "amount",
    "items_count",
    "night_order",
    "probability",
    "prediction"
)

# Print streaming results in the terminal
query = (
    results.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "checkpoint_orders")
    .start()
)

print("Listening to Kafka topic 'orders'.")
print("Send JSON order events from another terminal.")

query.awaitTermination()
PY
```

## 8. Start the Spark Structured Streaming Consumer

Open a third terminal:

```bash
cd ~/kafka_spark_ml
source ~/pyspark-env/bin/activate
```

Check the Spark version:

```bash
pyspark --version
```

For PySpark 4.2.0 with Scala 2.13, run:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  score_stream.py
```

Keep the consumer running.

**Important:** The Kafka connector version and Scala suffix must match the installed Spark version. If the Spark version is different, use its corresponding connector version.

## 9. Publish Order Events Using Kafka Console Producer

Open a fourth terminal:

```bash
export KAFKA_HOME="$HOME/kafka_2.13-4.3.1"
```

Start the Kafka console producer:

```bash
"$KAFKA_HOME/bin/kafka-console-producer.sh" \
  --topic orders \
  --bootstrap-server localhost:9092
```

Enter these JSON events one at a time:

```json
{"order_id":"201","amount":2500,"items_count":1,"night_order":0}
{"order_id":"202","amount":65000,"items_count":1,"night_order":1}
{"order_id":"203","amount":45000,"items_count":3,"night_order":1}
{"order_id":"204","amount":4500,"items_count":1,"night_order":0}
```

## 10. Verify the Streaming Predictions

Observe the Spark consumer terminal.

It should display incoming order events with:
- Order ID
- Amount
- Number of items
- Night order indicator
- Prediction probability
- Predicted class

Interpret the prediction:

| Prediction | Classification |
|---|---|
| 0 | Normal order |
| 1 | Suspicious order |

The exact prediction depends on the trained model and the input data.

## 11. Verify Kafka Messages

A separate terminal can be used to read messages from the topic:

```bash
"$KAFKA_HOME/bin/kafka-console-consumer.sh" \
  --topic orders \
  --from-beginning \
  --bootstrap-server localhost:9092
```

Press `Ctrl+C` to stop the consumer.

## 12. Stop the Applications

Stop the Spark streaming consumer using:

```text
Ctrl + C
```

Stop the Kafka producer using:

```text
Ctrl + C
```

Stop the Kafka broker using:

```text
Ctrl + C
```

## 13. Cleanup (Optional)

Remove the saved model and streaming checkpoint:

```bash
cd ~/kafka_spark_ml

rm -rf order_risk_model
rm -rf checkpoint_orders
```

Delete the Kafka topic if required:

```bash
"$KAFKA_HOME/bin/kafka-topics.sh" \
  --delete \
  --topic orders \
  --bootstrap-server localhost:9092
```

---

**End of Experiment 05**
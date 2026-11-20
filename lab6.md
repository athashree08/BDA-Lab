# Experiment 06 – ETL using Sqoop and Flume

## Aim

To configure Apache Flume to collect student activity log events and ingest them into HDFS, then verify the stored data using Hadoop commands.

## 1. Prerequisites

Required software:
- Java
- Hadoop
- HDFS
- Apache Flume
- MySQL Community Server (for the database preparation shown in the manual)

Check installations:

```bash
java -version
hadoop version
hdfs version
flume-ng version
mysql --version
```

Set the installation paths. Update these if your installation directories are different.

```bash
export HADOOP_HOME="$HOME/hadoop-3.5.0"
export FLUME_HOME="$HOME/bigdata-tools/apache-flume-1.11.0-bin"

export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$FLUME_HOME/bin:$PATH"
```

## 2. Start Hadoop Services

Start HDFS:

```bash
start-dfs.sh
```

Verify the Hadoop daemons:

```bash
jps
```

Create the HDFS directory:

```bash
hdfs dfs -mkdir -p /user/student/lab06
```

Verify:

```bash
hdfs dfs -ls /user/student
```

## 3. MySQL Database Preparation

Start the MySQL service:

```bash
sudo service mysql start
```

Log in to MySQL:

```bash
sudo mysql
```

Create the database:

```sql
CREATE DATABASE lab06_shop;

USE lab06_shop;
```

Create the customer table:

```sql
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(50),
    city VARCHAR(40),
    amount_spent DECIMAL(10,2)
);
```

Insert sample records:

```sql
INSERT INTO customers VALUES
(1, 'Athashree', 'Pune', 1200.50),
(2, 'Prathmesh', 'Pune', 850.00),
(3, 'Sattyam', 'Pune', 1600.75),
(4, 'Shweta', 'Pune', 950.25),
(5, 'Aarav', 'Mumbai', 700.00);
```

Display the records:

```sql
SELECT * FROM customers;
```

Exit MySQL:

```sql
EXIT;
```

## 4. Create the Student Activity Log

Create a directory for the practical:

```bash
mkdir -p ~/lab06
cd ~/lab06
```

Create the activity log:

```bash
cat > app.log <<'EOF'
2026-11-03 09:00:01 INFO User login successful
2026-11-03 09:00:05 INFO Product page opened
2026-11-03 09:00:12 WARN Payment retry requested
2026-11-03 09:00:18 INFO User logout successful
EOF
```

Display the log:

```bash
cat app.log
```

## 5. Create the Flume Configuration File

Create the configuration file:

```bash
nano ~/lab06/flume-lab06.conf
```

Add the following configuration:

```properties
# Agent configuration
agent1.sources = src1
agent1.channels = ch1
agent1.sinks = sink1

# Source configuration
agent1.sources.src1.type = exec
agent1.sources.src1.command = cat /home/$USER/lab06/app.log
agent1.sources.src1.channels = ch1

# Channel configuration
agent1.channels.ch1.type = file
agent1.channels.ch1.checkpointDir = /tmp/flume-checkpoint
agent1.channels.ch1.dataDirs = /tmp/flume-data

# Sink configuration
agent1.sinks.sink1.type = hdfs
agent1.sinks.sink1.hdfs.path = hdfs://localhost:9000/user/student/lab06/flume
agent1.sinks.sink1.hdfs.fileType = DataStream
agent1.sinks.sink1.hdfs.writeFormat = Text
agent1.sinks.sink1.hdfs.rollInterval = 0
agent1.sinks.sink1.hdfs.rollSize = 0
agent1.sinks.sink1.hdfs.rollCount = 4
agent1.sinks.sink1.channel = ch1
```

Save and exit.

**Note:** Replace `/home/$USER/lab06/app.log` with the actual absolute path to `app.log` if the configuration does not expand the environment variable.

Check the configuration:

```bash
cat ~/lab06/flume-lab06.conf
```

## 6. Start the Flume Agent

Ensure HDFS is running before starting Flume.

Run:

```bash
flume-ng agent \
  --conf "$FLUME_HOME/conf" \
  --conf-file ~/lab06/flume-lab06.conf \
  --name agent1 \
  -Dflume.root.logger=INFO,console
```

Keep the terminal running while Flume processes the log file.

## 7. Verify Data in HDFS

Open another terminal.

Check the Hadoop daemons:

```bash
jps
```

List the Flume output directory:

```bash
hdfs dfs -ls -R /user/student/lab06/flume
```

Display the ingested data:

```bash
hdfs dfs -cat /user/student/lab06/flume/*
```

The output should contain the student activity log events:

```text
2026-11-03 09:00:01 INFO User login successful
2026-11-03 09:00:05 INFO Product page opened
2026-11-03 09:00:12 WARN Payment retry requested
2026-11-03 09:00:18 INFO User logout successful
```

## 8. Stop the Flume Agent

Stop the Flume agent using:

```text
Ctrl + C
```

## 9. Cleanup (Optional)

Remove the HDFS output directory:

```bash
hdfs dfs -rm -r /user/student/lab06/flume
```

Remove local Flume checkpoint and data directories:

```bash
rm -rf /tmp/flume-checkpoint
rm -rf /tmp/flume-data
```

Stop Hadoop services if required:

```bash
stop-dfs.sh
```

---

## Result

A Flume agent was configured to ingest student activity log events into HDFS, and the stored data was verified using Hadoop filesystem commands.

---

**End of Experiment 06**
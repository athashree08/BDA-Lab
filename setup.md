
# Big Data Lab – Complete Setup Guide

This guide prepares a fresh Windows PC to execute all 10 Big Data practicals using WSL2 and Ubuntu.

## 1. Environment Overview

| Component | Standard environment |
|---|---|
| Host OS | Windows 11 |
| Linux environment | WSL2 |
| Linux distribution | Ubuntu 24.04 LTS |
| Java | OpenJDK 17 |
| Hadoop | 3.5.0 |
| Spark / PySpark | 4.2.0 |
| Kafka | 4.3.1 (KRaft mode) |
| Flume | 1.11.0 |
| Airflow | 3.1.8 |
| MongoDB | 8.0 Community |
| Cassandra | Apache Cassandra |
| MySQL | Ubuntu package |
| Python | Python 3.12 |
| Power BI | Windows application |

### Where applications will run

**Inside Ubuntu (WSL2):**
- Hadoop and HDFS
- Spark and PySpark
- Kafka
- Flume
- Airflow
- MongoDB
- Cassandra
- MySQL

**Inside Windows:**
- Power BI Desktop
- Web browser
- Windows Terminal

---

# PART A – WINDOWS AND WSL2 SETUP

## 2. Enable WSL2

Open PowerShell as Administrator.

Install WSL:

```powershell
wsl --install
```

Restart the computer if requested.

Install Ubuntu 24.04:

```powershell
wsl --install -d Ubuntu-24.04
```

Check the installed distributions:

```powershell
wsl --list --verbose
```

If Ubuntu is not using WSL2:

```powershell
wsl --set-version Ubuntu-24.04 2
```

Set Ubuntu as the default distribution:

```powershell
wsl --set-default Ubuntu-24.04
```

Open Ubuntu from the Start menu.

Create a Linux username and password when prompted.

## 3. Update Ubuntu

Run inside Ubuntu:

```bash
sudo apt update
sudo apt upgrade -y
```

Install common dependencies:

```bash
sudo apt install -y \
    build-essential \
    curl \
    wget \
    git \
    unzip \
    tar \
    gzip \
    gnupg \
    ca-certificates \
    software-properties-common \
    net-tools \
    openssh-server \
    openssh-client \
    rsync \
    vim \
    nano \
    tree \
    pkg-config
```

---

# PART B – JAVA SETUP

## 4. Install Java 17

```bash
sudo apt install -y openjdk-17-jdk
```

Verify:

```bash
java -version
javac -version
```

Configure JAVA_HOME:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
```

Reload the shell configuration:

```bash
source ~/.bashrc
```

Verify:

```bash
echo $JAVA_HOME
java -version
```

---

# PART C – PYTHON SETUP

## 5. Install Python and Virtual Environments

Ubuntu 24.04 includes Python 3.12.

Install the required packages:

```bash
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev
```

Verify:

```bash
python3 --version
pip3 --version
```

Create a common Python virtual environment:

```bash
mkdir -p ~/bigdata
python3 -m venv ~/bigdata/venv
```

Activate it:

```bash
source ~/bigdata/venv/bin/activate
```

Upgrade pip:

```bash
python -m pip install --upgrade pip setuptools wheel
```

Install common libraries:

```bash
pip install pandas numpy matplotlib scikit-learn requests
```

Verify:

```bash
python -c "import pandas, numpy, sklearn; print('Python libraries installed successfully')"
```

Whenever working with a practical that uses this environment:

```bash
source ~/bigdata/venv/bin/activate
```

---

# PART D – HADOOP SETUP

## 6. Download Hadoop

Create an installation directory:

```bash
mkdir -p ~/bigdata-tools
cd ~/bigdata-tools
```

Download Hadoop 3.5.0:

```bash
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
```

Extract:

```bash
tar -xzf hadoop-3.5.0.tar.gz
```

Set environment variables:

```bash
echo 'export HADOOP_HOME=$HOME/bigdata-tools/hadoop-3.5.0' >> ~/.bashrc
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
```

Reload:

```bash
source ~/.bashrc
```

Verify:

```bash
hadoop version
```

## 7. Configure Hadoop

Set Java in Hadoop configuration:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

Configure core-site.xml:

```bash
cat > $HADOOP_HOME/etc/hadoop/core-site.xml <<'EOF'
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF
```

Configure hdfs-site.xml:

```bash
mkdir -p ~/hadoop_data/namenode
mkdir -p ~/hadoop_data/datanode
```

```bash
cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>

    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://$HOME/hadoop_data/namenode</value>
    </property>

    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://$HOME/hadoop_data/datanode</value>
    </property>
</configuration>
EOF
```

Configure workers:

```bash
echo localhost > $HADOOP_HOME/etc/hadoop/workers
```

## 8. Configure SSH for Hadoop

Start SSH:

```bash
sudo service ssh start
```

Generate an SSH key:

```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
```

Authorize the key:

```bash
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
```

Test:

```bash
ssh localhost
```

If prompted, type `yes`.

Exit the SSH session:

```bash
exit
```

## 9. Initialize HDFS

Format the NameNode only once for a new Hadoop installation:

```bash
hdfs namenode -format
```

Start HDFS:

```bash
start-dfs.sh
```

Check the running daemons:

```bash
jps
```

Create a user directory:

```bash
hdfs dfs -mkdir -p /user/student
```

Check HDFS:

```bash
hdfs dfs -ls /
```

Web interfaces:

- NameNode: http://localhost:9870

Stop HDFS:

```bash
stop-dfs.sh
```

---

# PART E – SPARK AND PYSPARK SETUP

## 10. Install PySpark

Activate the Python environment:

```bash
source ~/bigdata/venv/bin/activate
```

Install PySpark 4.2.0:

```bash
pip install pyspark==4.2.0
```

Verify:

```bash
pyspark --version
```

Start PySpark:

```bash
pyspark
```

Test:

```python
numbers = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
numbers.collect()
```

Exit:

```python
exit()
```

Verify Spark from Python:

```bash
python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.appName('Test').getOrCreate(); print(s.version); s.stop()"
```

## 11. Spark Kafka Connector

For the practicals using Kafka and Spark Structured Streaming, use the connector matching Spark 4.2.0 and Scala 2.13:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
```

Example:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  application.py
```

The connector is downloaded automatically through Spark's package resolver.

---

# PART F – APACHE KAFKA SETUP

## 12. Download Kafka

```bash
cd ~/bigdata-tools
```

Download Kafka 4.3.1:

```bash
wget https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz
```

Extract:

```bash
tar -xzf kafka_2.13-4.3.1.tgz
```

Configure the environment variable:

```bash
echo 'export KAFKA_HOME=$HOME/bigdata-tools/kafka_2.13-4.3.1' >> ~/.bashrc
echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
```

Reload:

```bash
source ~/.bashrc
```

Verify:

```bash
kafka-topics.sh --version
```

## 13. Initialize Kafka KRaft Storage

Generate a cluster ID:

```bash
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
```

Format the storage:

```bash
kafka-storage.sh format \
    --standalone \
    -t "$KAFKA_CLUSTER_ID" \
    -c $KAFKA_HOME/config/server.properties
```

This is a one-time initialization for a fresh Kafka installation.

## 14. Start Kafka

```bash
kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Keep this terminal open.

Open another terminal and verify:

```bash
kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

## 15. Test Kafka Topic Creation

Create a test topic:

```bash
kafka-topics.sh \
    --create \
    --topic test-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1
```

List topics:

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

Delete the test topic:

```bash
kafka-topics.sh \
    --delete \
    --topic test-topic \
    --bootstrap-server localhost:9092
```

Stop Kafka:

```bash
Ctrl + C
```

---

# PART G – APACHE FLUME SETUP

## 16. Download Flume

```bash
cd ~/bigdata-tools
```

Download Flume 1.11.0:

```bash
wget https://archive.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz
```

Extract:

```bash
tar -xzf apache-flume-1.11.0-bin.tar.gz
```

Set environment variables:

```bash
echo 'export FLUME_HOME=$HOME/bigdata-tools/apache-flume-1.11.0-bin' >> ~/.bashrc
echo 'export PATH=$PATH:$FLUME_HOME/bin' >> ~/.bashrc
```

Reload:

```bash
source ~/.bashrc
```

Verify:

```bash
flume-ng version
```

## 17. Configure Hadoop Libraries for Flume

The Flume HDFS sink requires compatible Hadoop client libraries.

Set the Flume classpath:

```bash
export FLUME_CLASSPATH="$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*"
```

Use the Hadoop and Flume versions tested together before running the Lab 06 HDFS sink.

---

# PART H – APACHE AIRFLOW SETUP

## 18. Create a Separate Airflow Environment

Airflow should use its own virtual environment.

```bash
python3 -m venv ~/bigdata/airflow-env
```

Activate:

```bash
source ~/bigdata/airflow-env/bin/activate
```

Upgrade pip:

```bash
pip install --upgrade pip
```

Install Airflow 3.1.8 with Python 3.12 constraints:

```bash
pip install "apache-airflow==3.1.8" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt"
```

Install Pandas:

```bash
pip install pandas
```

Verify:

```bash
airflow version
```

## 19. Configure Airflow

Set Airflow home:

```bash
export AIRFLOW_HOME=~/airflow
```

Create the DAG directory:

```bash
mkdir -p $AIRFLOW_HOME/dags
```

Initialize the metadata database:

```bash
airflow db migrate
```

Start Airflow:

```bash
airflow standalone
```

Open the browser:

http://localhost:8080

The terminal displays the generated login credentials.

Keep the Airflow process running while executing Lab 07.

---

# PART I – MYSQL SETUP

## 20. Install MySQL

```bash
sudo apt install -y mysql-server
```

Start the service:

```bash
sudo service mysql start
```

Check:

```bash
sudo service mysql status
```

Verify:

```bash
mysql --version
```

Connect:

```bash
sudo mysql
```

Test:

```sql
SHOW DATABASES;
```

Exit:

```sql
EXIT;
```

---

# PART J – MONGODB SETUP

## 21. Install MongoDB Community Edition

Install prerequisites:

```bash
sudo apt install -y gnupg curl
```

Import the MongoDB 8.0 public key:

```bash
curl -fsSL https://pgp.mongodb.com/server-8.0.asc | \
sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
--dearmor
```

Add the Ubuntu 24.04 repository:

```bash
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" | \
sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
```

Update package information:

```bash
sudo apt update
```

Install MongoDB:

```bash
sudo apt install -y mongodb-org
```

Start MongoDB:

```bash
sudo systemctl start mongod
```

Enable automatic startup:

```bash
sudo systemctl enable mongod
```

Verify:

```bash
sudo systemctl status mongod
```

## 22. Connect to MongoDB

```bash
mongosh
```

Test:

```javascript
db.runCommand({ ping: 1 })
```

Exit:

```javascript
exit
```

---

# PART K – APACHE CASSANDRA SETUP

## 23. Install Cassandra

Cassandra requires a supported Java version. Use the OpenJDK 17 installation from Part B.

Install repository prerequisites:

```bash
sudo apt install -y curl gnupg
```

Import the Apache Cassandra repository key:

```bash
curl -o /tmp/cassandra.asc https://downloads.apache.org/cassandra/KEYS
```

```bash
sudo gpg --dearmor \
    -o /usr/share/keyrings/cassandra-archive-keyring.gpg \
    /tmp/cassandra.asc
```

Add the Cassandra repository:

```bash
echo "deb [signed-by=/usr/share/keyrings/cassandra-archive-keyring.gpg] https://debian.cassandra.apache.org 51x main" | \
sudo tee /etc/apt/sources.list.d/cassandra.sources.list
```

Update:

```bash
sudo apt update
```

Install:

```bash
sudo apt install -y cassandra
```

Start Cassandra:

```bash
sudo systemctl start cassandra
```

Check status:

```bash
sudo systemctl status cassandra
```

Verify the node:

```bash
nodetool status
```

## 24. Connect to Cassandra

```bash
cqlsh
```

Verify:

```sql
DESCRIBE CLUSTER;
```

Exit:

```sql
EXIT;
```

---

# PART L – POWER BI SETUP

## 25. Install Power BI Desktop

Power BI Desktop runs on Windows.

Download it from the official Microsoft website:

https://www.microsoft.com/en-us/power-platform/products/power-bi/desktop

Install Power BI Desktop.

Verify that it opens successfully.

For Lab 10, the Spark output CSV can be accessed from Windows through:

```text
\\wsl$\Ubuntu-24.04\home\<linux-username>\traffic_streaming\
```

Alternatively, save the CSV to a Windows-mounted directory such as:

```text
/mnt/c/Users/Public/
```

In Power BI:

1. Select Get Data.
2. Select Text/CSV.
3. Browse to the generated CSV.
4. Load the data.
5. Create the dashboard visuals.

---

# PART M – PROJECT DIRECTORY STRUCTURE

## 26. Create a Common Workspace

Inside Ubuntu:

```bash
mkdir -p ~/bigdata-labs
```

Create directories for all practicals:

```bash
mkdir -p ~/bigdata-labs/lab01
mkdir -p ~/bigdata-labs/lab02
mkdir -p ~/bigdata-labs/lab03
mkdir -p ~/bigdata-labs/lab04
mkdir -p ~/bigdata-labs/lab05
mkdir -p ~/bigdata-labs/lab06
mkdir -p ~/bigdata-labs/lab07
mkdir -p ~/bigdata-labs/lab08
mkdir -p ~/bigdata-labs/lab09
mkdir -p ~/bigdata-labs/lab10
```

Suggested workspace:

```text
bigdata-labs/
│
├── lab01/
├── lab02/
├── lab03/
├── lab04/
├── lab05/
├── lab06/
├── lab07/
├── lab08/
├── lab09/
└── lab10/
```

---

# PART N – FINAL VERIFICATION

## 27. Verify Common Software

Run these commands inside Ubuntu:

```bash
java -version
```

```bash
python3 --version
```

```bash
hadoop version
```

```bash
pyspark --version
```

```bash
kafka-topics.sh --version
```

```bash
flume-ng version
```

```bash
airflow version
```

```bash
mysql --version
```

```bash
mongosh --version
```

```bash
cqlsh --version
```

## 28. Important Environment Variables

Check configured paths:

```bash
echo $JAVA_HOME
echo $HADOOP_HOME
echo $HADOOP_CONF_DIR
echo $KAFKA_HOME
echo $FLUME_HOME
echo $AIRFLOW_HOME
```

If a variable is missing, check `~/.bashrc` and reload it:

```bash
source ~/.bashrc
```

## 29. Starting and Stopping Services

| Application | Start command | Stop command |
|---|---|---|
| HDFS | `start-dfs.sh` | `stop-dfs.sh` |
| Kafka | `kafka-server-start.sh $KAFKA_HOME/config/server.properties` | `Ctrl+C` |
| Airflow | `airflow standalone` | `Ctrl+C` |
| MySQL | `sudo service mysql start` | `sudo service mysql stop` |
| MongoDB | `sudo systemctl start mongod` | `sudo systemctl stop mongod` |
| Cassandra | `sudo systemctl start cassandra` | `sudo systemctl stop cassandra` |
| Flume | `flume-ng agent ...` | `Ctrl+C` |

## 30. Important Notes

1. Install and configure software only once.
2. Do not format the Hadoop NameNode every time you start HDFS.
3. Do not format Kafka storage every time you start Kafka.
4. Start HDFS before executing Flume HDFS sink operations.
5. Start Kafka before running Kafka producers or consumers.
6. Start the Spark streaming application before publishing events when using `startingOffsets=latest`.
7. Use the correct Kafka connector version for the installed Spark version.
8. Use a separate virtual environment for Airflow.
9. Keep MongoDB and Cassandra services running while executing their respective practicals.
10. Power BI Desktop runs on Windows, not inside the Ubuntu terminal.
11. Keep sufficient disk space for Hadoop data, Kafka logs, Spark checkpoints, and database files.
12. The replication factor of 1 is for a single-node learning environment only.

## Official Documentation

- WSL: https://learn.microsoft.com/en-us/windows/wsl/
- Hadoop: https://hadoop.apache.org/docs/
- Spark: https://spark.apache.org/docs/
- Kafka: https://kafka.apache.org/documentation/
- Flume: https://flume.apache.org/FlumeUserGuide.html
- Airflow: https://airflow.apache.org/docs/
- MongoDB: https://www.mongodb.com/docs/
- Cassandra: https://cassandra.apache.org/doc/
- Power BI: https://learn.microsoft.com/en-us/power-bi/

---

**End of Setup Guide**

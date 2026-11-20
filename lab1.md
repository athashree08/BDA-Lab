# Lab 01 – Hadoop Ecosystem and HDFS Operations

### Java installation verification

```cmd
java -version
javac -version
```

### Hadoop installation directory

```text
C:\Hadoop\hadoop-3.5.0
```

### Java directory

```text
C:\Java\jdk-17
```

### Environment variables

```text
JAVA_HOME=C:\Java\jdk-17
HADOOP_HOME=C:\Hadoop\hadoop-3.5.0
```

Add the following to the Windows PATH:

```text
%JAVA_HOME%\bin
%HADOOP_HOME%\bin
%HADOOP_HOME%\sbin
```

### Verify Hadoop installation

```cmd
hadoop version
```

## 4. WSL Ubuntu Setup

Open CMD:

```cmd
wsl
```

Verify Java:

```bash
java -version
```

Verify Hadoop:

```bash
hadoop version
```

Hadoop configuration directory:

```text
/home/athashree/hadoop-3.5.0/etc/hadoop
```

Set Hadoop configuration directory:

```bash
export HADOOP_CONF_DIR=$HOME/hadoop-3.5.0/etc/hadoop
```

## 5. Hadoop Configuration

### core-site.xml

File:

```text
~/hadoop-3.5.0/etc/hadoop/core-site.xml
```

Configured property:

```xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
</property>
```

## 6. Start Hadoop Services

```bash
start-dfs.sh
```

Verify running Java processes:

```bash
jps
```

Expected processes:

```text
NameNode
DataNode
SecondaryNameNode
```

## 7. HDFS Operations

Create user directory:

```bash
hdfs dfs -mkdir -p /user/athashree
```

List HDFS root:

```bash
hdfs dfs -ls /
```

Create Big Data directory:

```bash
hdfs dfs -mkdir /bigdata
```

Create local input file:

```bash
nano input.txt
```

Display local file:

```bash
cat input.txt
```

Upload file to HDFS:

```bash
hdfs dfs -put input.txt /bigdata
```

Verify uploaded file:

```bash
hdfs dfs -ls /bigdata
```

## 8. Practice Task – HDFS File Operations

Create student data directory:

```bash
hdfs dfs -mkdir -p /studentdata
```

Create local student file:

```bash
nano student.txt
```

Display local file:

```bash
cat student.txt
```

Upload file:

```bash
hdfs dfs -put student.txt /studentdata
```

Display file directly from HDFS:

```bash
hdfs dfs -cat /studentdata/student.txt
```

Copy file inside HDFS:

```bash
hdfs dfs -cp /studentdata/student.txt /studentdata/student_copy.txt
```

Rename copied file:

```bash
hdfs dfs -mv /studentdata/student_copy.txt /studentdata/Athashree_student.txt
```

Download renamed file:

```bash
hdfs dfs -get /studentdata/Athashree_student.txt student_downloaded.txt
```

Verify downloaded file:

```bash
cat student_downloaded.txt
```

Delete original file:

```bash
hdfs dfs -rm /studentdata/student.txt
```

Delete renamed file:

```bash
hdfs dfs -rm /studentdata/Athashree_student.txt
```

Delete empty directory:

```bash
hdfs dfs -rmdir /studentdata
```

Verify deletion:

```bash
hdfs dfs -ls /
```

## 9. Useful Verification Commands

```bash
java -version
javac -version
hadoop version
jps
hdfs dfs -ls /
hdfs dfs -ls /studentdata
hdfs dfs -du -h /
```

## 10. Stop Hadoop Services

```bash
stop-dfs.sh
```


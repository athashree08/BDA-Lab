# Lab 02 – Word Count using Hadoop MapReduce

## 3. Open WSL

From Windows CMD:

```cmd
wsl
```

Verify Java:

```bash
java -version
javac -version
```

Verify Hadoop:

```bash
hadoop version
```

## 4. Start Hadoop Services

```bash
start-dfs.sh
```

Verify running services:

```bash
jps
```

Expected processes:

```text
NameNode
DataNode
SecondaryNameNode
```

## 5. Locate Hadoop MapReduce Examples JAR

```bash
find ~/hadoop-3.5.0 -name "hadoop-mapreduce-examples*.jar"
```

Expected JAR location:

```text
/home/athashree/hadoop-3.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar
```

## 6. Create Input File

```bash
cd ~
nano input.txt
```

Sample input:

```text
Hadoop is big data
Hadoop is powerful
Big data is important
```

Save and verify:

```bash
cat input.txt
```

## 7. Upload Input File to HDFS

Create the input directory:

```bash
hdfs dfs -mkdir -p /wordcount/input
```

Upload the file:

```bash
hdfs dfs -put input.txt /wordcount/input/
```

Verify:

```bash
hdfs dfs -ls /wordcount/input
```

Display the HDFS file:

```bash
hdfs dfs -cat /wordcount/input/input.txt
```

## 8. Execute Hadoop Word Count

```bash
hadoop jar ~/hadoop-3.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar wordcount /wordcount/input /wordcount/output
```

Note: The output directory must not already exist before running the job.

If running again, remove the previous output:

```bash
hdfs dfs -rm -r /wordcount/output
```

Then execute the Word Count command again.

## 9. Verify MapReduce Output

List the output directory:

```bash
hdfs dfs -ls /wordcount/output
```

Display the reducer output:

```bash
hdfs dfs -cat /wordcount/output/part-r-00000
```

Expected word counts:

```text
Big         1
Hadoop      2
big         1
data        2
important   1
is          3
powerful    1
```

Note: Word Count is case-sensitive. `Big` and `big` are counted separately.

## 10. Practice Task – Custom Word Count

Create a custom input file:

```bash
nano custom_input.txt
```

Add at least 8–10 lines of text.

Create an HDFS input directory:

```bash
hdfs dfs -mkdir -p /wordcount/custom_input
```

Upload the custom file:

```bash
hdfs dfs -put custom_input.txt /wordcount/custom_input/
```

Verify the uploaded file:

```bash
hdfs dfs -cat /wordcount/custom_input/custom_input.txt
```

Run Word Count:

```bash
hadoop jar ~/hadoop-3.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar wordcount /wordcount/custom_input /wordcount/custom_output
```

Display custom Word Count results:

```bash
hdfs dfs -cat /wordcount/custom_output/part-r-00000
```

## 11. Useful Verification Commands

```bash
java -version
hadoop version
jps
hdfs dfs -ls /
hdfs dfs -ls /wordcount/input
hdfs dfs -ls /wordcount/output
hdfs dfs -cat /wordcount/output/part-r-00000
```

## 12. Stop Hadoop Services

```bash
stop-dfs.sh
```

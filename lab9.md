# Experiment 09 – Cassandra Database Operations and Data Modeling

## Aim

To create a Cassandra keyspace and table, model data using partition and clustering keys, and perform basic insert and query operations.

## 1. Prerequisites

Required software:
- Apache Cassandra
- Cassandra Query Language Shell (cqlsh)
- Java

Check Java:

```bash
java -version
```

Check Cassandra:

```bash
cassandra -v
```

Check cqlsh:

```bash
cqlsh --version
```

## 2. Start Cassandra

Start the Cassandra service:

```bash
sudo systemctl start cassandra
```

Check the service status:

```bash
sudo systemctl status cassandra
```

Verify the Cassandra node:

```bash
nodetool status
```

## 3. Connect to Cassandra

Open the Cassandra Query Language Shell:

```bash
cqlsh
```

Check the connection:

```sql
DESCRIBE CLUSTER;
```

## 4. Create a Keyspace

Create a keyspace named `university`:

```sql
CREATE KEYSPACE university
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

Select the keyspace:

```sql
USE university;
```

Verify:

```sql
DESCRIBE KEYSPACE university;
```

**Note:** Replication factor 1 is suitable only for this single-node lab demonstration, not production resilience.

## 5. Create the Attendance Table

Create a table named `attendance`:

```sql
CREATE TABLE attendance (
    student_id TEXT,
    course_id TEXT,
    attendance_date DATE,
    status TEXT,
    PRIMARY KEY ((student_id, course_id), attendance_date)
);
```

Describe the table:

```sql
DESCRIBE TABLE attendance;
```

The primary key contains:
- Partition key: `student_id`, `course_id`
- Clustering key: `attendance_date`

## 6. Insert Attendance Records

Insert attendance records for student S101:

```sql
INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S101', 'DS301', '2026-10-01', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S101', 'DS301', '2026-10-02', 'Absent');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S101', 'DS301', '2026-10-03', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S101', 'DS301', '2026-10-04', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S101', 'DS301', '2026-10-05', 'Present');
```

Insert attendance records for student S102:

```sql
INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S102', 'DS301', '2026-10-01', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S102', 'DS301', '2026-10-02', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S102', 'DS301', '2026-10-03', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S102', 'DS301', '2026-10-04', 'Absent');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S102', 'DS301', '2026-10-05', 'Absent');
```

Insert attendance records for student S103:

```sql
INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S103', 'DS301', '2026-10-01', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S103', 'DS301', '2026-10-02', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S103', 'DS301', '2026-10-03', 'Absent');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S103', 'DS301', '2026-10-04', 'Present');

INSERT INTO attendance
(student_id, course_id, attendance_date, status)
VALUES ('S103', 'DS301', '2026-10-05', 'Present');
```

## 7. Retrieve Attendance Records

Retrieve attendance history for student S101 in course DS301:

```sql
SELECT *
FROM attendance
WHERE student_id = 'S101'
AND course_id = 'DS301';
```

Retrieve attendance history for student S102:

```sql
SELECT *
FROM attendance
WHERE student_id = 'S102'
AND course_id = 'DS301';
```

Retrieve attendance history for student S103:

```sql
SELECT *
FROM attendance
WHERE student_id = 'S103'
AND course_id = 'DS301';
```

## 8. Update Attendance

Update S101's attendance status for 2 October 2026:

```sql
UPDATE attendance
SET status = 'Present'
WHERE student_id = 'S101'
AND course_id = 'DS301'
AND attendance_date = '2026-10-02';
```

Verify the updated record:

```sql
SELECT *
FROM attendance
WHERE student_id = 'S101'
AND course_id = 'DS301';
```

## 9. Delete an Attendance Record

Delete S101's attendance record for 2 October 2026:

```sql
DELETE FROM attendance
WHERE student_id = 'S101'
AND course_id = 'DS301'
AND attendance_date = '2026-10-02';
```

Verify:

```sql
SELECT *
FROM attendance
WHERE student_id = 'S101'
AND course_id = 'DS301';
```

## 10. Create a Second Table for Date-Based Queries

The first table is designed to retrieve attendance history for a particular student and course.

To retrieve all students absent on a particular course date, create another table named `attendance_by_date`.

```sql
CREATE TABLE attendance_by_date (
    course_date DATE,
    student_id TEXT,
    course_id TEXT,
    status TEXT,
    PRIMARY KEY (course_date, student_id, course_id)
) WITH CLUSTERING ORDER BY (
    student_id ASC,
    course_id ASC
);
```

Describe the table:

```sql
DESCRIBE TABLE attendance_by_date;
```

## 11. Insert Records into the Second Table

Insert attendance records for S101:

```sql
INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-01', 'S101', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-02', 'S101', 'DS301', 'Absent');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-03', 'S101', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-04', 'S101', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-05', 'S101', 'DS301', 'Present');
```

Insert attendance records for S102:

```sql
INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-01', 'S102', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-02', 'S102', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-03', 'S102', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-04', 'S102', 'DS301', 'Absent');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-05', 'S102', 'DS301', 'Absent');
```

Insert attendance records for S103:

```sql
INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-01', 'S103', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-02', 'S103', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-03', 'S103', 'DS301', 'Absent');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-04', 'S103', 'DS301', 'Present');

INSERT INTO attendance_by_date
(course_date, student_id, course_id, status)
VALUES ('2026-10-05', 'S103', 'DS301', 'Present');
```

## 12. Find Absent Students on a Given Date

Retrieve all attendance records for 3 October 2026:

```sql
SELECT *
FROM attendance_by_date
WHERE course_date = '2026-10-03';
```

Retrieve only absent students:

```sql
SELECT *
FROM attendance_by_date
WHERE course_date = '2026-10-03'
AND status = 'Absent'
ALLOW FILTERING;
```

**Note:** `ALLOW FILTERING` is used here because `status` is not part of the primary key. For a production query, a separate table designed specifically for absent-student lookups would be preferable.

## 13. Verify the Second Table

Display all records:

```sql
SELECT *
FROM attendance_by_date;
```

Display records for a particular date:

```sql
SELECT *
FROM attendance_by_date
WHERE course_date = '2026-10-03';
```

## 14. Exit Cassandra

```sql
EXIT;
```

Stop Cassandra if required:

```bash
sudo systemctl stop cassandra
```

---

## Result

A Cassandra keyspace and attendance tables were created successfully. Attendance records were inserted, queried, updated, and deleted. Partition and clustering keys were used to design tables around different query requirements.

---

**End of Experiment 09**
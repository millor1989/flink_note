### 时区

Flink 提供了丰富的日期和时间数据类型，Flink 支持以 session 为级别设置时区。这些时间戳数据类型和 Flink 的时区支持使得跨时区处理业务数据变得简单。

#### 1、TIMESTAMP vs TIMESTAMP_LTZ

##### 1.1、TIMESTAMP 类型

- `TIMESTAMP(p)` 是 `TIMESTAMP(p) WITHOUT TIME ZONE` 的缩写，精度 `p` 的取值范围是闭区间 `0` ~ `9`，默认值是 `6`。

- `TIMESTAMP` 描述了一个代表年、月、日、时、分、秒以及小数秒的时间戳。

- `TIMESTAMP` 可以用一个字符串字面量来指定，比如：

  ```sql
  Flink SQL> SELECT TIMESTAMP '1970-01-01 00:00:04.001';
  +-------------------------+
  | 1970-01-01 00:00:04.001 |
  +-------------------------+
  ```

##### 1.2、TIMESTAMP_LTZ 类型

- `TIMESTAMP_LTZ(p)` 是 `TIMESTAMP(p) WITH LOCAL TIME ZONE` 的缩写，精度 `p` 的取值范围是闭区间 `0` ~ `9`，默认值是 `6`。

- `TIMESTAMP_LTZ` 描述了一个时间线（time-line）上的绝对时间点，它保存了一个表示纪元毫秒的 long 值和一个表示纳秒毫秒（nanosecond-of-millisecond）的 int 值。纪元时间是以标准的 Java 纪元 `1970-01-01T00:00:00Z` 为基准的。每个数据都被编译为当前会话的本地时区时间，以进行运算和展示。

- `TIMESTAMP_LTZ` 可以不能用字面量来指定，但是可以从一个 long 类型的纪元时间获取（比如，Java `System.currentTimeMillis()` 产生的 long 时间）。

  ```sql
  Flink SQL> CREATE VIEW T1 AS SELECT TO_TIMESTAMP_LTZ(4001, 3);
  Flink SQL> SET 'table.local-time-zone' = 'UTC';
  Flink SQL> SELECT * FROM T1;
  +---------------------------+
  | TO_TIMESTAMP_LTZ(4001, 3) |
  +---------------------------+
  |   1970-01-01 00:00:04.001 |
  +---------------------------+
  
  Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
  Flink SQL> SELECT * FROM T1;
  +---------------------------+
  | TO_TIMESTAMP_LTZ(4001, 3) |
  +---------------------------+
  |   1970-01-01 08:00:04.001 |
  +---------------------------+
  ```

- `TIMESTAMP_LTZ` 可以用于跨时区的业务，因为绝对时间点（比如，上面例子中的 4001 毫秒）描述的是不同时区中相同的时间点。在相同时间点的前提下，世界上所有机器 `System.currentTimeMillis()` 返回的都是相同的值，这就是绝对时间的含义。

#### 2、时区使用

本地时区定义了当前会话的时区 id。

SQL 客户端定义时区：

```sql
-- set to UTC time zone
Flink SQL> SET 'table.local-time-zone' = 'UTC';

-- set to Shanghai time zone
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';

-- set to Los_Angeles time zone
Flink SQL> SET 'table.local-time-zone' = 'America/Los_Angeles';
```

Scala 定义时区：

```scala
val envSetting = EnvironmentSettings.inStreamingMode()
val tEnv = TableEnvironment.create(envSetting)

// set to UTC time zone
tEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

// set to Shanghai time zone
tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

// set to Los_Angeles time zone
tEnv.getConfig.setLocalTimeZone(ZoneId.of("America/Los_Angeles"))
```

Flink SQL 中会话时区很有用，主要用途有：

##### 2.1、决定时间函数的返回值

如下函数是受配置的时区影响的：

- LOCALTIME
- LOCALTIMESTAMP
- CURRENT_DATE
- CURRENT_TIME
- CURRENT_TIMESTAMP
- CURRENT_ROW_TIMESTAMP()
- NOW()
- PROCTIME()

```sql
Flink SQL> SET 'sql-client.execution.result-mode' = 'tableau';
Flink SQL> CREATE VIEW MyView1 AS SELECT LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_ROW_TIMESTAMP(), NOW(), PROCTIME();
Flink SQL> DESC MyView1;
```

```
+------------------------+-----------------------------+-------+-----+--------+-----------+
|                   name |                        type |  null | key | extras | watermark |
+------------------------+-----------------------------+-------+-----+--------+-----------+
|              LOCALTIME |                     TIME(0) | false |     |        |           |
|         LOCALTIMESTAMP |                TIMESTAMP(3) | false |     |        |           |
|           CURRENT_DATE |                        DATE | false |     |        |           |
|           CURRENT_TIME |                     TIME(0) | false |     |        |           |
|      CURRENT_TIMESTAMP |            TIMESTAMP_LTZ(3) | false |     |        |           |
|CURRENT_ROW_TIMESTAMP() |            TIMESTAMP_LTZ(3) | false |     |        |           |
|                  NOW() |            TIMESTAMP_LTZ(3) | false |     |        |           |
|             PROCTIME() | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |        |           |
+------------------------+-----------------------------+-------+-----+--------+-----------+
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
Flink SQL> SELECT * FROM MyView1;
```

```
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| LOCALTIME |          LOCALTIMESTAMP | CURRENT_DATE | CURRENT_TIME |       CURRENT_TIMESTAMP | CURRENT_ROW_TIMESTAMP() |                   NOW() |              PROCTIME() |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
|  15:18:36 | 2021-04-15 15:18:36.384 |   2021-04-15 |     15:18:36 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 | 2021-04-15 15:18:36.384 |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView1;
```

```
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
| LOCALTIME |          LOCALTIMESTAMP | CURRENT_DATE | CURRENT_TIME |       CURRENT_TIMESTAMP | CURRENT_ROW_TIMESTAMP() |                   NOW() |              PROCTIME() |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
|  23:18:36 | 2021-04-15 23:18:36.384 |   2021-04-15 |     23:18:36 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 | 2021-04-15 23:18:36.384 |
+-----------+-------------------------+--------------+--------------+-------------------------+-------------------------+-------------------------+-------------------------+
```

##### 2.2、`TIMESTAMP_LTZ` 的字符串表示

`TIMESTAMP_LTZ` 值转换为字符串格式、转换为 `STRING` 类型、`TIMESTAMP`类型时，或者 `TIMESTAMP` 转换为 `TIMESTAMP_LTZ`时，也用到会话时区。

```sql
Flink SQL> CREATE VIEW MyView2 AS SELECT TO_TIMESTAMP_LTZ(4001, 3) AS ltz, TIMESTAMP '1970-01-01 00:00:01.001'  AS ntz;
Flink SQL> DESC MyView2;
```

```
+------+------------------+-------+-----+--------+-----------+
| name |             type |  null | key | extras | watermark |
+------+------------------+-------+-----+--------+-----------+
|  ltz | TIMESTAMP_LTZ(3) |  true |     |        |           |
|  ntz |     TIMESTAMP(3) | false |     |        |           |
+------+------------------+-------+-----+--------+-----------+
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
Flink SQL> SELECT * FROM MyView2;
```

```
+-------------------------+-------------------------+
|                     ltz |                     ntz |
+-------------------------+-------------------------+
| 1970-01-01 00:00:04.001 | 1970-01-01 00:00:01.001 |
+-------------------------+-------------------------+
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView2;
```

```
+-------------------------+-------------------------+
|                     ltz |                     ntz |
+-------------------------+-------------------------+
| 1970-01-01 08:00:04.001 | 1970-01-01 00:00:01.001 |
+-------------------------+-------------------------+
```

```sql
Flink SQL> CREATE VIEW MyView3 AS SELECT ltz, CAST(ltz AS TIMESTAMP(3)), CAST(ltz AS STRING), ntz, CAST(ntz AS TIMESTAMP_LTZ(3)) FROM MyView2;
```

```
Flink SQL> DESC MyView3;
+-------------------------------+------------------+-------+-----+--------+-----------+
|                          name |             type |  null | key | extras | watermark |
+-------------------------------+------------------+-------+-----+--------+-----------+
|                           ltz | TIMESTAMP_LTZ(3) |  true |     |        |           |
|     CAST(ltz AS TIMESTAMP(3)) |     TIMESTAMP(3) |  true |     |        |           |
|           CAST(ltz AS STRING) |           STRING |  true |     |        |           |
|                           ntz |     TIMESTAMP(3) | false |     |        |           |
| CAST(ntz AS TIMESTAMP_LTZ(3)) | TIMESTAMP_LTZ(3) | false |     |        |           |
+-------------------------------+------------------+-------+-----+--------+-----------+
```

```sql
Flink SQL> SELECT * FROM MyView3;
```

```
+-------------------------+---------------------------+-------------------------+-------------------------+-------------------------------+
|                     ltz | CAST(ltz AS TIMESTAMP(3)) |     CAST(ltz AS STRING) |                     ntz | CAST(ntz AS TIMESTAMP_LTZ(3)) |
+-------------------------+---------------------------+-------------------------+-------------------------+-------------------------------+
| 1970-01-01 08:00:04.001 |   1970-01-01 08:00:04.001 | 1970-01-01 08:00:04.001 | 1970-01-01 00:00:01.001 |       1970-01-01 00:00:01.001 |
+-------------------------+---------------------------+-------------------------+-------------------------+-------------------------------+
```

#### 3、时间属性和时区

##### 3.1、处理时间和时区

Flink SQL 使用 `PROCTIME()` 函数定义处理事件属性，函数返回类型是 `TIMESTAMP_LTZ`。

>在 Flink 1.13之前，`PROCTIME()` 的返回值类型是 `TIMESTAMP`，并且是 UTC 时区的 `TIMESTAMP`，比如，上海时间是  `2021-03-01 12:00:00` 而 `PROCTIME()` 返回的时间是  `2021-03-01 04:00:00` 。Flink 1.13 修复了这个问题，用 `TIMESTAMP_LTZ` 作为 `PROCTIME()` 的返回时间类型，用户不用再处理时区问题了。

`PROCTIME()` 总是表示本地时间戳的值，使用 `TIMESTAMP_LTZ` 类型也能很好的支持夏令时（DayLight Saving Time）。

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
Flink SQL> SELECT PROCTIME();
```

```
+-------------------------+
|              PROCTIME() |
+-------------------------+
| 2021-04-15 14:48:31.387 |
+-------------------------+
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT PROCTIME();
```

```
+-------------------------+
|              PROCTIME() |
+-------------------------+
| 2021-04-15 22:48:31.387 |
+-------------------------+
```

```sql
Flink SQL> CREATE TABLE MyTable1 (
                  item STRING,
                  price DOUBLE,
                  proctime as PROCTIME()
            ) WITH (
                'connector' = 'socket',
                'hostname' = '127.0.0.1',
                'port' = '9999',
                'format' = 'csv'
           );

Flink SQL> CREATE VIEW MyView3 AS
            SELECT
                TUMBLE_START(proctime, INTERVAL '10' MINUTES) AS window_start,
                TUMBLE_END(proctime, INTERVAL '10' MINUTES) AS window_end,
                TUMBLE_PROCTIME(proctime, INTERVAL '10' MINUTES) as window_proctime,
                item,
                MAX(price) as max_price
            FROM MyTable1
                GROUP BY TUMBLE(proctime, INTERVAL '10' MINUTES), item;

Flink SQL> DESC MyView3;
```

```
+-----------------+-----------------------------+-------+-----+--------+-----------+
|           name  |                        type |  null | key | extras | watermark |
+-----------------+-----------------------------+-------+-----+--------+-----------+
|    window_start |                TIMESTAMP(3) | false |     |        |           |
|      window_end |                TIMESTAMP(3) | false |     |        |           |
| window_proctime | TIMESTAMP_LTZ(3) *PROCTIME* | false |     |        |           |
|            item |                      STRING | true  |     |        |           |
|       max_price |                      DOUBLE |  true |     |        |           |
+-----------------+-----------------------------+-------+-----+--------+-----------+
```

在终端中为 `MyTable1` 注入数据：

```
> nc -lk 9999
A,1.1
B,1.2
A,1.8
B,2.5
C,3.8
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
Flink SQL> SELECT * FROM MyView3;
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView3;
```

与使用 UTC 时区相比返回的是不同的窗口开始时间、窗口结束时间、和窗口处理时间：

```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_procime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.005 |    A |       1.8 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.007 |    B |       2.5 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:10:00.007 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

>处理时间窗口是不确定的，所以每次运行会得到不同的窗口和不同的聚合结果。这个例子只是用来展示时区是如何营销处理时间窗口的。

##### 3.2、事件时间和时区

Flink 支持基于 TIMESTAMP 列和 TIMESTAMP_LTZ 列定义事件时间。

##### 3.2.1、基于 TIMESTAMP 的事件时间属性

如果数据源中的时间戳数据是表示为年、月、日、时、分、秒的，通常是没有时区信息的字符串值，比如 `2020-04-15 20:13:40.564`，推荐将事件时间属性定义为一个 `TIMESTAMP` 列。

```sql
Flink SQL> CREATE TABLE MyTable2 (
                  item STRING,
                  price DOUBLE,
                  ts TIMESTAMP(3), -- TIMESTAMP data type
                  WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
            ) WITH (
                'connector' = 'socket',
                'hostname' = '127.0.0.1',
                'port' = '9999',
                'format' = 'csv'
           );

Flink SQL> CREATE VIEW MyView4 AS
            SELECT
                TUMBLE_START(ts, INTERVAL '10' MINUTES) AS window_start,
                TUMBLE_END(ts, INTERVAL '10' MINUTES) AS window_end,
                TUMBLE_ROWTIME(ts, INTERVAL '10' MINUTES) as window_rowtime,
                item,
                MAX(price) as max_price
            FROM MyTable2
                GROUP BY TUMBLE(ts, INTERVAL '10' MINUTES), item;

Flink SQL> DESC MyView4;
```

```
+----------------+------------------------+------+-----+--------+-----------+
|           name |                   type | null | key | extras | watermark |
+----------------+------------------------+------+-----+--------+-----------+
|   window_start |           TIMESTAMP(3) | true |     |        |           |
|     window_end |           TIMESTAMP(3) | true |     |        |           |
| window_rowtime | TIMESTAMP(3) *ROWTIME* | true |     |        |           |
|           item |                 STRING | true |     |        |           |
|      max_price |                 DOUBLE | true |     |        |           |
+----------------+------------------------+------+-----+--------+-----------+
```

从终端往 `MyTable2` 中导入数据：

```
> nc -lk 9999
A,1.1,2021-04-15 14:01:00
B,1.2,2021-04-15 14:02:00
A,1.8,2021-04-15 14:03:00
B,2.5,2021-04-15 14:04:00
C,3.8,2021-04-15 14:05:00
C,3.8,2021-04-15 14:11:00
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
Flink SQL> SELECT * FROM MyView4;
```

```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView4;
```

即使改变会话时区，返回结果也是和 UTC 时区计算结果相同：

```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

##### 3.2.2、基于 TIMESTAMP_LTZ 的事件时间属性

如果数据源中的时间戳数据是表示为一个纪元时间的，通常是一个 long 值，比如，`1618989564564`，推荐定义事件时间属性为 `TIMESTAMP_LTZ` 字段。

```sql
Flink SQL> CREATE TABLE MyTable3 (
                  item STRING,
                  price DOUBLE,
                  ts BIGINT, -- long time value in epoch milliseconds
                  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
                  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '10' SECOND
            ) WITH (
                'connector' = 'socket',
                'hostname' = '127.0.0.1',
                'port' = '9999',
                'format' = 'csv'
           );

Flink SQL> CREATE VIEW MyView5 AS
            SELECT
                TUMBLE_START(ts_ltz, INTERVAL '10' MINUTES) AS window_start,
                TUMBLE_END(ts_ltz, INTERVAL '10' MINUTES) AS window_end,
                TUMBLE_ROWTIME(ts_ltz, INTERVAL '10' MINUTES) as window_rowtime,
                item,
                MAX(price) as max_price
            FROM MyTable3
                GROUP BY TUMBLE(ts_ltz, INTERVAL '10' MINUTES), item;

Flink SQL> DESC MyView5;
```

```
+----------------+----------------------------+-------+-----+--------+-----------+
|           name |                       type |  null | key | extras | watermark |
+----------------+----------------------------+-------+-----+--------+-----------+
|   window_start |               TIMESTAMP(3) | false |     |        |           |
|     window_end |               TIMESTAMP(3) | false |     |        |           |
| window_rowtime | TIMESTAMP_LTZ(3) *ROWTIME* |  true |     |        |           |
|           item |                     STRING |  true |     |        |           |
|      max_price |                     DOUBLE |  true |     |        |           |
+----------------+----------------------------+-------+-----+--------+-----------+
```

输入数据：

```
A,1.1,1618495260000  # The corresponding utc timestamp is 2021-04-15 14:01:00
B,1.2,1618495320000  # The corresponding utc timestamp is 2021-04-15 14:02:00
A,1.8,1618495380000  # The corresponding utc timestamp is 2021-04-15 14:03:00
B,2.5,1618495440000  # The corresponding utc timestamp is 2021-04-15 14:04:00
C,3.8,1618495500000  # The corresponding utc timestamp is 2021-04-15 14:05:00
C,3.8,1618495860000  # The corresponding utc timestamp is 2021-04-15 14:11:00
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'UTC';
Flink SQL> SELECT * FROM MyView5;
```

```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    A |       1.8 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    B |       2.5 |
| 2021-04-15 14:00:00.000 | 2021-04-15 14:10:00.000 | 2021-04-15 14:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

```sql
Flink SQL> SET 'table.local-time-zone' = 'Asia/Shanghai';
Flink SQL> SELECT * FROM MyView5;
```



```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    A |       1.8 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    B |       2.5 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

返回结果也是和 UTC 时区计算结果不同：

```
+-------------------------+-------------------------+-------------------------+------+-----------+
|            window_start |              window_end |          window_rowtime | item | max_price |
+-------------------------+-------------------------+-------------------------+------+-----------+
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    A |       1.8 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    B |       2.5 |
| 2021-04-15 22:00:00.000 | 2021-04-15 22:10:00.000 | 2021-04-15 22:09:59.999 |    C |       3.8 |
+-------------------------+-------------------------+-------------------------+------+-----------+
```

#### 4、夏令时（DST）的支持

Flink SQL 支持基于 `TIMESTAMP_LTZ` 字段定义时间属性，基于此，Flink SQL 可以在窗口处理中优雅地使用 `TIMESTAMP` 和 `TIMESTAMP_LTZ` 类型以支持夏令时。

Flink 使用时间戳字面量来划分窗口，根据每行的纪元时间为窗口分配数据。这意味着，Flink 的窗口开始和窗口结束（比如，`TUMBLE_START` 和 `TUMBLE_END`）使用 `TIMESTAMP` 类型，窗口时间属性使用 `TIMESTAMP_LTZ` 类型。如下，翻滚窗口的例子，洛杉矶的夏令时从 `2021-03-14 02:00:00` 开始：

```
long epoch1 = 1615708800000L; // 2021-03-14 00:00:00
long epoch2 = 1615712400000L; // 2021-03-14 01:00:00
long epoch3 = 1615716000000L; // 2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
long epoch4 = 1615719600000L; // 2021-03-14 04:00:00
```

翻滚窗口  [2021-03-14 00:00:00, 2021-03-14 00:04:00] 会收集到洛杉矶时区 3 个小时的数据。但是，在另一个非夏令时时区会收集到 4 个小时的数据，用户要做的只是基于 `TIMESTAMP_LTZ` 列定义时间属性。

Flink 的所有窗口都是这样的，Flink SQL 中的所有操作都很好地支持 `TIMESTAMP_LTZ`，因此 Flink 可以很好地支持夏令时时区。

#### 5、批模式和流模式的不同

如下时间函数：

- LOCALTIME
- LOCALTIMESTAMP
- CURRENT_DATE
- CURRENT_TIME
- CURRENT_TIMESTAMP
- NOW()

Flink 会根据执行模式计算它们的值。在流模式中，它们是每一条记录分别计算的值得，但是批模式中，是查询开始时计算值，并且每一行都使用相同的值。。

如下函数，无论那种模式，都是分别为每一行计算值的：

- CURRENT_ROW_TIMESTAMP()
- PROCTIME()
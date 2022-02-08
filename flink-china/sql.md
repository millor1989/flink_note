### Flink SQL

#### 1、Flink SQL 的特点

- 声明式 API：Flink 最高层的 API，易于使用
- 流批统一：一样的 SQL，一样的结果
- 自动优化：屏蔽 State 的复杂性，自动做到最优处理
- 用途广泛：ETL，统计分析，实时报表，实时风控

#### 2、Flink SQL 应用示例

示例环境，Docker，资源 3 - 4 GB，3 - 4 CPUs

##### 启动 Flink SQL CLI：

```shell
docker-compose exec sql-client ./sql-client.sh
```

Docker Compose 预先注册了表和数据，可以通过 `show talbes` 查看。示例用到了表 `Rides`——出租车行车记录数据流（Kafka 数据流，包含时间和位置信息）。查看表结构：

```sql
Flink SQL> DESCRIBE Rides;
root
 |-- rideId: Long           // 行为 ID (包含两条记录，一条入一条出）
 |-- taxiId: Long           // 出租车 ID 
 |-- isStart: Boolean       // 开始 or 结束
 |-- lon: Float             // 经度
 |-- lat: Float             // 纬度
 |-- rideTime: TimeIndicatorTypeInfo(rowtime)     // 时间
 |-- psgCnt: Integer        // 乘客数
```

##### 示例 1：过滤 Filter

查看发生在纽约的行车记录

Docker 环境中预定一了一些函数，比如 `isInNYC(lon, lat)` 确定一个经纬度坐标是否在纽约，`toAreaId(lon, lat)` 将经纬度转换为区块。

因此，查询纽约的行车记录，只需在 SQL CLI 中运行如下查询：

```sql
SELECT * FROM Rides WHERE isInNYC(lon, lat)
```

SQL CLI 会提交 SQL 任务到 Docker 集群中，从数据源不断拉取数据，并通过 `isInNYC` 过滤出所需要的数据，SQL CLI 会进入可视化模式并不断展示过滤后的结果。也可以通过 [Web UI](http://localhost:8081) 查看 Flink 作业的运行情况。

##### 示例 2：Group Aggregate

计算在纽约搭载每种乘客数量的行车事件数（即搭载 1 个乘客、2 个乘客 ... 的行车事件数）。

按照乘客数 `psgCnt` 做分组：

```sql
SELECT psgCnt, COUNT(*) AS cnt 
FROM Rides 
WHERE isInNYC(lon, lat)
GROUP BY psgCnt;
```

##### 示例 3：Window Aggregate

为了持续监测纽约的交通流量，需要计算每个区块每 5 分钟进入的车辆数——只关心有超过 5 辆车子进入的区块。

“每 5 分钟”——需要用到 Tumbling Window 语法；“每个区块”——需要用 `toAreaId` 函数进行分组计算；`isStart` 字段表示行程开始。

```sql
SELECT 
  toAreaId(lon, lat) AS area, 
  TUMBLE_END(rideTime, INTERVAL '5' MINUTE) AS window_end, 
  COUNT(*) AS cnt 
FROM Rides 
WHERE isInNYC(lon, lat) and isStart
GROUP BY 
  toAreaId(lon, lat), 
  TUMBLE(rideTime, INTERVAL '5' MINUTE) 
HAVING COUNT(*) >= 5;
```

##### Window Aggregate 与 Group Aggregate 的区别

![窗口](/assets/1644299969008.png)

window Aggregation 计算每小时每个用户点击的次数：

![1644300050381](/assets/1644300050381.png)

Group Aggregation 从历史到现在每个用户点击的次数：

![1644300128265](/assets/1644300128265.png)

Window Aggregate 是 window 结束时才输出，输出结果是最终值，不会再进行修改，其输出流是 **Append 流**。Group Aggregate 则是没处理一条数据，就输出最新的结果，输出结果是不断更新的，其输出流是 **Update 流**。

另外，window 是有 watermark 的，可以知道哪些窗口已经过期，所以可以及时清理过期的状态，保证状态维持在稳定的大小。而 Group Aggregate 不知道哪些数据会过期，所以状态会无限增长，对于生产环境作业来说是不稳定因素，建议对 Group Aggregate 的作业配置 State TTL。

![1644300231302](/assets/1644300231302.png)

例如统计每个店铺每天的实时 PV，可以将 TTL 设置为 24+ 小时，因为一天前的状态一般是用不到了。

```sql
SELECT  DATE_FORMAT(ts, 'yyyy-MM-dd'), shop_id, COUNT(*) as pv
FROM T
GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd'), shop_id
```

需要注意，如果 TTL 设置的太小，可能会漏掉一些有用的状态和数据，从而导致数据精确性的问题，这是需要视情况做权衡的。

##### 示例 4：将 Append 流写入 Kafka

在 Flink 中，目前 Update 流只能写入支持更新的外部存储，比如 MySQL、HBase、ElasticSearch。Append 流可以写入任意存储，一般写入日志类型的系统，比如 Kafka。

这里的需求是将每 10 分钟的搭乘的乘客数写入 Kafka。

已经预定义了 Kafka 结果 Table Sink_TenMinPsgCnts，每 10 分钟的搭乘的乘客数可以用 Tumbling Window 来描述：

```sql
INSERT INTO Sink_TenMinPsgCnts 
SELECT 
  TUMBLE_START(rideTime, INTERVAL '10' MINUTE) AS cntStart,  
  TUMBLE_END(rideTime, INTERVAL '10' MINUTE) AS cntEnd,
  CAST(SUM(psgCnt) AS BIGINT) AS cnt 
FROM Rides 
GROUP BY TUMBLE(rideTime, INTERVAL '10' MINUTE);
```

##### 示例 5：将 Update 流写入 ElasticSearch

将每个区域出发的行车数写入 ES。

已经预定义了 ES 结果表 Sink_AreaCnts——只有两个字段 areaId 和 cnt。

```sql
INSERT INTO Sink_AreaCnts 
SELECT toAreaId(lon, lat) AS areaId, COUNT(*) AS cnt 
FROM Rides 
WHERE isStart
GROUP BY toAreaId(lon, lat);
```

在 SQL CLI 执行该查询后，ES 会自动创建 area-cnts 索引。可以通过 ES 查询写入结果。


### 流概念[#](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/concepts/overview/)

Flink 的 Table API 和 SQL 是流处理和批处理统一的 APIs。这意味着，Table API 和 SQL 查询有相同的语义——无论它们的输入是有边界的批输入还是无边界的流输入。

#### 1、状态管理

表程序可以用一个状态后端（state backend）和多种检查点选项来进行配置，以根据状态大小和容错来处理不同的需求。可以使用一个运行中的 Table API & SQL 管道的 savepoint 来在之后的时间里及时地恢复应用的状态。

##### 1.1、状态使用

由于 Table API & SQL 程序的声明式本质，在管道中什么地方、使用了多少状态通常不明显。planner 决定了是否需要状态来计算正确的结果。根据当前的优化器（optimizer）规则管道被优化为使用尽可能少的状态。

>从概念上讲，源表从不整个地保存在状态中。实现者处理逻辑表（即，动态表）。它们的状态需求由使用的操作决定。

像 `SELECT ... FROM ... WHERE` 这样的，只是由字段映射和过滤组成的查询通常是无状态的管道。但是像连接、聚合、或者去重这类操作，需要将中间结果保留到容错的存储中，都用到了 Flink 的状态。

>对应的算子文档会介绍需要多少的状态，以及如何限制潜在的不断增长的状态大小。

比如，常规的 SQL 连接需要将两个输入表整个地保存到状态。为了正确的 SQL 语义，运行时需要假设两个表的匹配会在任何时间点发生。Flink 利用水印的概念，提供了 [optimized window and interval joins](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/joins/) 旨在保存在较小的状态。

计算每个会话点击数量的例子：

```sql
SELECT sessionId, count(*) FROM clicks GROUP BY sessionId;
```

`sessionId` 属性被用作分组的键，持续的查询为每个 `sessionId` 维持着一个计数。`sessionId` 属性随着时间增长，`sessionId` 对应的计数值只在会话活跃时有效，即，只需要维持一段时间。但是，持续的查询无法知道 `sessionId` 的活跃时间，并且期望每个 `sessionId` 可以在任何时间点出现。它为每个观测到的 `sessionId` 维持着一个计数值。最后，随着观测到的 `sessionId` 越来越多，查询的总状态大小是持续增长的。

##### 1.1.1、空闲状态留存时间（Idle State Retention Time)

`table.exec.state.ttl` 定义了一个键的状态如果没有被更新，它在多久之后会被移除。

对于之前的查询例子，`sessionId` 对应的计数如果在配置的时长内没有被更新，它就会被移除。

##### 1.2、有状态的升级和演变（Stateful ）

以流模式运行的表程序是旨在作为 *长期查询*（standing queries）的，意味着定义一次，然后作为静态的端到端管道持续地进行运算。

对于有状态的管道，查询或者 Flink 的 planner 的任何改变都可能导致完全不同的执行计划。这使得表程序的有状态的升级和演变面临挑战。Flink 社区致力于改善这些缺点。

比如，增加了一个过滤操作、优化器可能会决定重新排序连接或者改变一个中间算子的 schema。这会因为算子状体的拓扑改变或者不同的字段结构，而无法从保存点恢复。

查询实现者必须保证升级或演变前后，优化后的计划（optimized plans）是相互兼容的。使用 SQL 的 `EXPLAIN` 命令或者 Table API 的 `table.explain()` 来进行检查。

由于，新的优化器规则是持续地加入的，因而算子变得更加高效和专业化，这也导致新的 Flink 版本可能导致优化后的计划不兼容。

>目前，框架不能保证状态可以从一个保存点映射到新的表算子拓扑。
>
>换句话说，仅仅查询和 Flink 版本都保持一致的情况下才支持保存点。

由于 Flink 社区拒绝在小版本迭代中（比如，从 1.13.1 到 1.13.2）中修改优化的计划和算子拓扑，小版本升级对 Table API & SQL 管道（的状态恢复）来说是安全的。可是，大版本升级（比如，从 1.12 到 1.13）是不支持的。

对这两个缺点（即，修改查询和修改 Flink 版本），建议在开始尽心实时数据处理之前，使用历史数据来检验更新后的表程序的状态是否可以“热身”（即，初始化）。Flink 社区在开发一个混合源（hybird source）来使得这种切换尽可能地简便。

#### 2、动态表（Dynamic Tables）

##### 2.1、数据流的关系型查询（Relational Queries on Data Streams）

传统关系代数（relational algebra）和流处理作为输入数据、执行、输出结果方面的比较：

| 关系代数 / SQL                                               | 流处理                                                     |
| ------------------------------------------------------------ | ---------------------------------------------------------- |
| 关系（Relations，or tables）是有限的（多个）元组集合         | 流是元组的无限序列                                         |
| 查询是在批数据（比如，关系型数据库中的一个表）上进行的，能够访问整个的输入数据 | 流查询启动之后无法访问所有的数据，并且必须“等待”数据流进来 |
| 批查询在产生了一个固定大小的结果之后就结束了                 | 流查询基于接收到的记录持续地更新它的结果，从不结束         |

尽管存在这些区别，关系查询和 SQL 提供了处理流的强大工具集。高级的关系型数据库系统提供了一个叫作 **物化视图**（Materialized Views）的特性。物化视图是被定义为一个 SQL 查询，就像一个常规的虚拟视图。但是，物化视图缓存了查询结果，被访问的时候不用运行查询。缓存的挑战是，防止缓存提供过期的结果。当视图查询的基础表被修改，物化视图就变得过时了。急切视图维护（*Eager View Maintenance*）是视图基础表更新时马上更新物化视图的计术。

考虑到如下几点时，急切视图维护与流的 SQL 查询之间的联系就变得明显了：

- `INSERT`、`UPDATE`、`DELETE` DML 语句的流处理的数据库表结果，通常叫作 *changelog stream*。
- 物化视图被定义为一个 SQL 查询。要更新视图，查询必须持续地处理视图的基础表的 changelog stream。
- 物化视图是流 SQL 查询的结果。

##### 2.2、动态表和持续查询（Dynamic Tables & Continuous Queries）

动态表是 Flink 的 Table API 和 SQL 支持流数据的核心概念。与代表批数据的静态表不同，动态表随着时间变化。但是与静态批处理表相似，系统可以对动态表进行查询。查询动态表产生了持续查询。持续查询从不结束，并且产生另一个动态表。查询持续地更新它的动态结果表以反映它的输入表的变化。本质上，动态表的动态查询与定义物化视图的查询非常类似。

持续查询输出总是在语义上等价于相同查询以批模式在输入表快照上执行的结果。

下图展示了流、动态表、和持续查询的关系：

![Dynamic tables](/assets/stream-query-stream.png)

1. 流转换为动态表
2. 持续查询在动态表上执行产生一个新的动态表
3. 结果动态表转换为一个流

>动态表只是一个逻辑概念，在查询执行期间动态表不必（完全地）物化

接下来，将使用有如下 schema 的点击事件流来解释动态表和持续查询的概念：

```sql
CREATE TABLE clicks (
  user  VARCHAR,     -- the name of the user
  url   VARCHAR,     -- the URL that was accessed by the user
  cTime TIMESTAMP(3) -- the time when the URL was accessed
) WITH (...);
```

##### 2.3、基于流定义一个表（Defining a Table on a Stream）

使用关系性查询处理流需要将其转换为一个 `Table`。这样，流的每一条记录会被解释为对结果表的一个 `INSERT` 修改。接下来，基于一个仅 `INSERT` 的 changelog 流构建了一个表。

下图展示了点击事件流（左侧）是如何转换为一个表（右侧）的。随着更多的记录插入到点击流中，结果表也是持续增长的。

![Append mode](/assets/append-mode.png)

>要记住，基于流定义的表本质上不是物化的。

##### 2.3.1、持续查询（Continuous Queries）

持续查询对动态表进行运算，并产生一个新的动态表作为结果。与批查询不同，持续查询从不结束，并且根据输入表的更新来更新结果表。在任一时间点，持续查询在语义上等价于以批模式运行与输入表快照上的相同查询。

**例 1** 基于 `user` 属性对表 `clicks` 进行分组，统计访问的 URLs 的数量。下图展示了随着 `clicks` 表中数据记录的增加查询是如何运算的。

![Continuous Non-Windowed Query](/assets/query-groupBy-cnt.png)

当查下启动时，`clicks` 表（左侧）是空的。当插入第一条记录时，查询计算结果表。第一条记录 `[Mary, ./home]` 到达，结果表（右侧，第一行）由一行 `[Mary, 1]` 构成。当第二行 `[Bob, ./cart]` 插入到 `clicks` 表时，查询会更新结果表并插入新的一行 `[Bob, 1]`。第三行，`[Mary, ./prod?id=1]` 导致对已经计算过的结果的更新——`[Mary, 1]` 被更新为 `[Mary, 2]`。最后，当第四行插入到 `clicks` 表时，查询往结果表插入第三行 `[Liz, 1]`。

**例 2** 与 例 1 类似，多了一个每小时滚动的窗口作为分组条件。下图展现了不同时间点的输入和输出来将动态表变化的本质展示出来。

![Continuous Group-Window Query](/assets/query-groupBy-window-cnt.png)

查询每个小时持续地更新结果表。在 `12:00:00` 到 `12:59:59` 之间 `clicks` 表包含四行。根据输入，查询计算出两条结果（每个 `user` 一条）并插入到结果表。对于下一个窗口 `13:00:00` 至 `13:59:59`，`clicks` 表包含三行，结果是又有两条记录插入到结果表。随着时间变化，结果表 `clicks` 会被插入更多的记录。

##### 2.3.2、更新和追加查询（Update and Append Queries）

前面的两个例子尽管很相似，但是在一个关键方面还是不同的：

- 例 1 会更新之前输出的结果，即，定义结果表的 changelog 流包含了 `INSERT` 和 `UPDATE` 变化。
- 例 2 只会往结果表追加数据，即，结果标的 changelog 流仅包含 `INSERT` 变化。

查询产生仅追加的表（append-only table）还是更新的表（updated table）有如下含义：

- 进行更新改变的查询通常必须维持更多的状态。
- 仅追加表到流的转换和更新表到流的转换是不同的。

##### 2.3.3、查询限制（Query Restrictions）

许多（但是不是所有）语义上合法的查询可以作为对流的持续查询运行。某些查询运行起来开销太大，比如需要维护的状态大小太大或者运算更新（computing updates）开销太大。

- **状态大小（State Size）**：持续查询是在无限流上运行的，通常可能运行几周或几个月。因此，持续查询处理的数据总量可能非常巨大。必须更新之前输出结果的查询需要维护所有输出的行才能对它们进行更新。比如，例 1 的查询需要保存每个用户的 URL 数量，然后当输入表收到新的行时将新的结果输出。如果仅仅追踪注册的用户，可能需要维护的记录数量不会太大；可是，如果未注册用户有唯一的用户名，需要维护的记录数量就会随着时间增长并且可能最后会导致查询失败。

  ```sql
  SELECT user, COUNT(url)
  FROM clicks
  GROUP BY user;
  ```

- **运算更新（Computing Updates）**：对于某些查询，即使仅仅输入记录更新或插入一条记录，也需要重新运算并更新已输出结果的一大部分。这些查询不太适合作为持续查询运行。比如下面的查询，根据用户最后一次点击的时间计算用户的 `RANK`。只要 `clicks` 表收到一条新的记录，用户的 `lastAction` 就会更新并计算一个新的 `RANK`。但是，两条不可能有相同的 `RANK`，所有 `RANK` 较小的记录都需要被更新。

  ```sql
  SELECT user, RANK() OVER (ORDER BY lastAction)
  FROM (
    SELECT user, MAX(cTime) AS lastAction FROM clicks GROUP BY user
  );
  ```

[查询配置](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/config/) 介绍了控制持续查询运行的参数。某些参数可以用于以维护的状态的大小来换取结果的精确。

##### 2.4、表到流的转换

动态表可以像常规的数据库表一样被持续地 `INSERT`、`UPDATE`、`DELETE`。当将一个动态表转换为流或者写到外部系统时，需要对这些变化进行编码（encode）。Flink 的 Table API 和 SQL 支持三种方式编码动态表的变化进行编码：

- **Append-only stream**：对于只会被 `INSERT` 变化修改的动态表，通过输出插入的行可以将其转换位流。
- **Retract stream**：撤回流有两种消息类型，*增加消息（add messages）* 和 *撤回消息（retract messages）*。动态表转换为撤回流时，`INSERT` 变化被编码为增加消息，`DELETE` 变化被编码为撤回消息， `UPDATE` 变化被编码为一个更新（之前）记录的撤回消息和一个更新（新的）记录的增加消息（additional message，应该等价于 add message）。下图展示了动态表到撤回流的转换。![Dynamic tables](/assets/undo-redo-mode.png)
- **Upsert stream**：上插流是有 *上插消息（upsert messages）* 和 *删除消息（delete messages）* 两种消息类型的流。被转换为上插流的动态表需要一个（可能是组合的）唯一键（unique key）。具有唯一键的动态表被转换为流时，`INSERT` 和 `UPDATE` 变化被编码为上插消息，`DELETE` 变化被编码为删除消息。流消费算子需要注意唯一键属性以正确的消费消息。与撤回流的主要不同是 `UPDATE` 变化被编码为一条消息所以更高效。下图展示了动态表到上插流的转换。![Dynamic tables](https://nightlies.apache.org/flink/flink-docs-release-1.15/fig/table-streaming/redo-mode.png)

将动态表转换为 `DataStream` 的 API，在之前的章节 [概念和通用 API](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/common/#convert-a-table-into-a-datastream) 中已经介绍过。要注意，当将动态表转换为 `DataStream` 时只支持追加流和撤回流。将动态表发出到外部系统的 `TableSink` 接口将在 [表源和表汇](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sourcessinks/#dynamic-table-sink) 一章介绍。

#### 3、时间属性

Flink 可以基于不同的时间概念处理数据：处理事件（Processing time）和事件时间（Event time）。

##### 3.1、时间属性简介

时间属性可以是每个表 schema 的一部分。它们是在通过 `CREATE TABLE` DDL 或 `DataStream` 创建表时定义的。一旦定义了时间属性，就可以将其作为一个字段来引用，并可以在基于时间的操作中使用。只要时间属性不被修改，只是简单地从查询的一个部分转移到另一个部分，它都是一个有效的时间属性。时间属性行为与常规的时间戳类似，并且可以用于运算。当在运算中使用时，时间属性会被物化并作为标准的时间戳。但是，普通的时间戳不能用来替换或者转换为时间属性。

##### 3.2、事件时间

事件时间允许表结构基于每条记录中的时间戳来产生结果，即使记录顺序混乱或者有迟到事件，也能产生一致的结果。当从持久性存储中读取记录时，它还能确保表程序结果的可重播性。

此外，就事件时间而言，批环境和流环境中的表程序语法是统一的。流环境中的时间属性可以是批环境中行的常规列。

为了处理流中的乱序事件、区分准时和迟到事件，Flink 需要知道每行的时间戳，and it also needs regular indications of how far along in event time the processing has processed so far(via so-called watermarks).

事件时间属性可以在 `CREATE` 表的 DDL 中定义，也可以在 DataStream 到表的转换中定义。

##### 3.2.1、在 DDL 中定义

事件时间属性是在 `CREATE` 表的 DDL 中，用一个 `WATERMARK` 语句来定义的。水印语句定义一个基于事件时间字段水印生成表达式，它将事件时间字段标记为事件时间属性。水印语句和水印策略的详情参考 [CREATE TABLE DDL](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/create/#create-table)。

Flink 支持基于 TIMESTAMP 列和 TIMESTAMP_LIZ 列定义事件时间属性。如果源中的时间戳数据是表示为年-月-日-时-分-秒形式——通常是没有时区信息的字符串，比如：2020-05-20 22:20:40.567，那么建议定义事件时间属性为一个 TIMESTAMP 列：

```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time TIMESTAMP(3),
  -- declare user_action_time as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
```

如果源中的时间戳数据是由一个纪元时间（epoch time）表示的——通常是一个 long 值，比如：1618989564564，那么推荐定义事件时间属性为一个 TIMESTAMP_LIZ 列：

```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  ts BIGINT,
  time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),
  -- declare time_ltz as event time attribute and use 5 seconds delayed watermark strategy
  WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND
) WITH (
  ...
);

SELECT TUMBLE_START(time_ltz, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(time_ltz, INTERVAL '10' MINUTE);
```

##### 3.2.2、在 DataStream 到 Table 转换期间定义

当转换 `DataStream` 为表时，在 schema 定义期间可以使用 `.rowtime` 属性定义事件时间属性。被转换的 `DataStream` 中必须已经分配过时间戳和水印。在转换期间，Flink 总是获取 rowtime 属性为 TIMESTAMP WITHOUT TIME ZONE，因为 **DataStream 没有时区的概念，并且以 UTC 时区对待所有的事件时间**。

当转换 `DataStream` 为表时，有两种方式定义时间属性。根据在 `DataStream` 的 schema 中是否存在指定的 `.rowtime` 字段名，时间戳要么作为新列追加，要么替换已经存在的列。

不管哪种方式，事件时间时间戳字段都会持有 `DataStream` 事件时间时间戳的值。

```scala
// 方式 1:

// extract timestamp and assign watermarks based on knowledge of the stream
val stream: DataStream[(String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// declare an additional logical field as an event time attribute
val table = tEnv.fromDataStream(stream, $"user_name", $"data", $"user_action_time".rowtime)


// 方式 2:

// extract timestamp from first field, and assign watermarks based on knowledge of the stream
val stream: DataStream[(Long, String, String)] = inputStream.assignTimestampsAndWatermarks(...)

// the first field has been used for timestamp extraction, and is no longer necessary
// replace first field with a logical event time attribute
val table = tEnv.fromDataStream(stream, $"user_action_time".rowtime, $"user_name", $"data")

// Usage:

val windowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
```

##### 3.3、处理时间

使用处理时间时，表程序基于本地机器的时间产生结果。是最简单的时间概念，但是产生的结果不确定。处理时间不需要时间戳提取或者水印的生成。

##### 3.3.1、DDL 中定义

在 `CREATE` 表的 DDL 中，使用系统函数 `PROCTIME()` 定义处理时间属性为一个运算字段（computed column），函数返回类型是 TIMESTAMP_LIZ。运算字段的详情参考 [CREATE TABLE DDL](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/create/#create-table)。

```sql
CREATE TABLE user_actions (
  user_name STRING,
  data STRING,
  user_action_time AS PROCTIME() -- declare an additional field as a processing time attribute
) WITH (
  ...
);

SELECT TUMBLE_START(user_action_time, INTERVAL '10' MINUTE), COUNT(DISTINCT user_name)
FROM user_actions
GROUP BY TUMBLE(user_action_time, INTERVAL '10' MINUTE);
```

##### 3.3.2、在 DataStream 到 Table 转换期间定义

在 schema 定义期间使用 `.proctime` 属性定义处理时间属性。时间属性必须仅仅是通过一个额外的逻辑字段扩展物理 schema。因此，只能在 schema 定义的末尾定义。

```scala
val stream: DataStream[(String, String)] = ...

// declare an additional logical field as a processing time attribute
val table = tEnv.fromDataStream(stream, $"UserActionTimestamp", $"user_name", $"data", $"user_action_time".proctime)

val windowedTable = table.window(Tumble over 10.minutes on $"user_action_time" as "userActionWindow")
```

#### 4、版本化的表（Versioned Tables）

Flink SQL 操作的动态表可能会被追加或者更新。版本化表代表了一个特别类型的更新表——记得每个键的过往值。

##### 4.1、概念

动态表定义了随时间变化的关系。通常，尤其是当使用元数据时，一个键的值改变时，旧值不会变得不相关。

Flink SQL 可以基于动态表用一个 `PRIMARY KEY` 限制和时间属性来定义版本化表。

Flink 中的主键限制意味着，表或视图的一列或者一组列是唯一且非 NULL 的。上插表（upserting table）的主键，意味着代表某一行随着时间推移变化的，某一个特定键（INSERT/UPDATE/DELETE）的具体变化。上插表的时间属性定义了每个变化发生的时间。

综合在一起，Flink 可以追踪某一行随着时间的变化，并维护某个键某段时间哪个值是有效的。

假设有一张表追踪商店里不同产品的价格：

```sql
(changelog kind)  update_time  product_id product_name price
================= ===========  ========== ============ ===== 
+(INSERT)         00:01:00     p_001      scooter      11.11
+(INSERT)         00:02:00     p_002      basketball   23.11
-(UPDATE_BEFORE)  12:00:00     p_001      scooter      11.11
+(UPDATE_AFTER)   12:00:00     p_001      scooter      12.99
-(UPDATE_BEFORE)  12:00:00     p_002      basketball   23.11 
+(UPDATE_AFTER)   12:00:00     p_002      basketball   19.99
-(DELETE)         18:00:00     p_001      scooter      12.99 
```

根据这一组变化，追踪 scooter 价格随着时间如何变化。

如果查询这个表不同时间不同产品的价格，会得到不同的结果。比如，在 10:00:00 查询的结果是：

```sql
update_time  product_id product_name price
===========  ========== ============ ===== 
00:01:00     p_001      scooter      11.11
00:02:00     p_002      basketball   23.11
```

而 13:00:00 查询结果是：

```sql
update_time  product_id product_name price
===========  ========== ============ ===== 
12:00:00     p_001      scooter      12.99
12:00:00     p_002      basketball   19.99
```

##### 4.2、版本化表源

版本化表对所有（底层源或者格式直接定义了 changelogs 的）表都是隐式地定义的。例子包括，upsert Kafka 源以及数据库 changelog 格式——比如 debezium 和 canal。之前已经提到，唯一的额外需要是 `CREATE` 表语句必须包含一个 `PRIMARY KEY` 和一个事件时间属性。

```sql
CREATE TABLE products (
	product_id    STRING,
	product_name  STRING,
	price         DECIMAL(32, 2),
	update_time   TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
	PRIMARY KEY (product_id) NOT ENFORCED,
	WATERMARK FOR update_time AS update_time
) WITH (...);
```

##### 4.3、版本化表视图

如果底层查询包含一个唯一键限制和事件时间属性， Flink 也支持定义版本化的视图。假设有一张仅追加的汇率表 `currency_rates`。

```sql
CREATE TABLE currency_rates (
	currency      STRING,
	rate          DECIMAL(32, 10),
	update_time   TIMESTAMP(3),
	WATERMARK FOR update_time AS update_time
) WITH (
	'connector' = 'kafka',
	'topic'	    = 'rates',
	'properties.bootstrap.servers' = 'localhost:9092',
	'format'    = 'json'
);
```

`currency_rates` 表中每种相对于 USD 的汇率一行，每当汇率变化是收到一个新的行。`JSON` 格式本身不支持 changelog 语义，所以 Flink 只能以仅追加的形式读这个表。

```sql
(changelog kind) update_time   currency   rate
================ ============= =========  ====
+(INSERT)        09:00:00      Yen        102
+(INSERT)        09:00:00      Euro       114
+(INSERT)        09:00:00      USD        1
+(INSERT)        11:15:00      Euro       119
+(INSERT)        11:49:00      Pounds     108
```

Flink 将每一行翻译为对表的 `INSERT`，意味着不能对汇率定义一个 `PRIMARY KEY`。可是，主键对于定义版本化的表是必须的。通过定义一个 [去重查询](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/deduplication/#deduplication)（用推断的主键（currency）和事件时间（update_time）产生一个有序的 changelog 流） Flink 可以将这个表翻译为一个版本化表。

```sql
-- Define a versioned view
CREATE VIEW versioned_rates AS              
SELECT currency, rate, update_time              -- (1) `update_time` keeps the event time
  FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY currency  -- (2) the inferred unique key `currency` can be a primary key
         ORDER BY update_time DESC) AS rownum 
      FROM currency_rates)
WHERE rownum = 1; 

-- the view `versioned_rates` will produce a changelog as the following.
(changelog kind) update_time currency   rate
================ ============= =========  ====
+(INSERT)        09:00:00      Yen        102
+(INSERT)        09:00:00      Euro       114
+(INSERT)        09:00:00      USD        1
+(UPDATE_AFTER)  10:45:00      Euro       116
+(UPDATE_AFTER)  11:15:00      Euro       119
+(INSERT)        11:49:00      Pounds     108
```

Flink 有一个特殊的优化步骤，能够高效地将这个查询转换为可以在后续查询中使用的版本化的表。通常，如下格式的查询的结果会产生一个版本化的表：

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr DESC) AS rownum
   FROM table_name)
WHERE rownum = 1
```

**参数说明**：

- `ROW_NUMBER()`：为每一行分配一个唯一的、从 1 开始的、连续的号码
- `PARTITION BY col1[, col2...]`：指定分区列，即，去重的键。这些列组成了后续的版本化表的主键。
- `ORDER BY time_attr DESC`：指定排序列，必须是一个时间属性
- `WHERE rownum = 1`：`rownum = 1` 对于 Flink 识别这个查询以产生一个版本化表来说是必须的。

#### 5、临时表函数（Temporal Table Function）

使用临时表函数可以访问一个临时表某一特定时间点的版本。为了访问临时表中的数据，必须传递一个决定返回表的版本的时间属性。Flink 使用 [表函数](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/functions/udfs/#table-functions) 的 SQL 语法作为表示它的方法。

与版本化表不同，临时表函数只能基于仅追加的流定义——不支持 changelog 输入。另外，临时表函数可以用纯粹的 SQL DDL 定义。

##### 5.1、定义一个临时表函数

使用 Table API 可以基于仅追加流定义临时表函数。用一个或者多个主要列（key columns）和一个用于版本划分的时间属性来注册临时表函数。

假设有一个仅追加表 `currency_rates`，：

```sql
SELECT * FROM currency_rates;

update_time   currency   rate
============= =========  ====
09:00:00      Yen        102
09:00:00      Euro       114
09:00:00      USD        1
11:15:00      Euro       119
11:49:00      Pounds     108
```

那么可以使用 Table API 注册临时表函数，用 `currency` 作为键、`update_time` 作为划分版本的时间属性：

```scala
rates = tEnv
    .from("currency_rates").
    .createTemporalTableFunction("update_time", "currency")
 
tEnv.registerFunction("rates", rates)
```

##### 5.2、临时表函数连接（Temporal Table Function Join）

定义过临时表函数之后，可以将其作为标准的表函数使用。仅追加表（left input/probe side）可以和临时表（right input/build side）连接。

假设有一张追踪顾客不同货币订单的仅追加表 `orders`：

```sql
SELECT * FROM orders;

order_time amount currency
========== ====== =========
10:15        2    Euro
10:30        1    USD
10:32       50    Yen
10:52        3    Euro
11:04        5    USD
```

用这两张表，将订单货币统一为——USD：

```scala
val result = orders
    .joinLateral($"rates(order_time)", $"orders.currency = rates.currency")
    .select($"(o_amount * r_rate).sum as amount"))
```


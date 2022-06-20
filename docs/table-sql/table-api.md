### Table API

Table API 是用于流处理和批处理的统一的、关系型 API。Table API 的查询不用修改，就可以在批或者流输入上运行。Table API 是 SQL 语言的超集（super set）专门被设计为在 Apache Flink 上运行。Table API 是Scala、Java、Python 语言集成的 API。Table API 不是通过 SQL 字符串指定查询，而是用 Java、Scala、Python 以语言嵌入（language-embedded）风格定义查询的。

接下来的例子中，假设有一个注册过的表 `Orders` ——属性为 `a`，`b`，`c`，`rowtime`。`rowtime` 字段为流模式下的一个逻辑时间属性或者是批处理模式下的一个常规的时间戳字段。

#### 1、概览和例子（Overview & Examples）

批环境下执行的一个表程序，扫描 `Orders` 表，按照 `a` 字段分组，计算每组的行数。

通过导入 `org.apache.flink.table.api._`， `org.apache.flink.api.scala._`，和 `org.apache.flink.table.api.bridge.scala._`（用于与 DataStream 桥接）开启对 Scala Table API 的支持。

```scala
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

// environment configuration
val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    .build()

val tEnv = TableEnvironment.create(settings)

// register Orders table in table environment
// ...

// specify table program
val orders = tEnv.from("Orders") // schema (a, b, c, rowtime)

val result = orders
               .groupBy($"a")
               .select($"a", $"b".count as "cnt")
               .execute()
               .print()
```

如下为一个复杂点的例子：

```scala
// environment configuration
// ...

// specify table program
val orders: Table = tEnv.from("Orders") // schema (a, b, c, rowtime)

val result: Table = orders
        .filter($"a".isNotNull && $"b".isNotNull && $"c".isNotNull)
        .select($"a".lowerCase() as "a", $"b", $"rowtime")
        .window(Tumble over 1.hour on $"rowtime" as "hourlyWindow")
        .groupBy($"hourlyWindow", $"a")
        .select($"a", $"hourlyWindow".end as "hour", $"b".avg as "avgBillingAmount")
```

#### 2、操作

Table API 支持如下的操作。但是，不是所有的操作在流、批模式下都可以。

##### 2.1、扫描、映射、过滤

##### 2.1.1、From

###### 两种模式下都可用

扫描一个注册过的表：

```scala
val orders = tableEnv.from("Orders")
```

##### 2.1.2、FromValues

###### 两种模式下都可用

用提供的行生成一个表，可以用 `row(...)` 表达式创建组合行：

```scala
table = tEnv.fromValues(
   row(1, "ABC"),
   row(2L, "ABCDE")
)
```

生成表的 schema 如下：

```
root
|-- f0: BIGINT NOT NULL     // original types INT and BIGINT are generalized to BIGINT
|-- f1: VARCHAR(5) NOT NULL // original types CHAR(3) and CHAR(5) are generalized
                            // to VARCHAR(5). VARCHAR is used instead of CHAR so that
                            // no padding is applied
```

这个方法可以自动地从输入表达式提取类型。如果某个位置类型不同，这个方法会尝试为所有类型找一个共同的超类型（common super type）。如果不存在共同的超类型，则抛出错误。

也可以明确地指定请求的类型。对于分配更宽泛的类型（more generic types）比如 `Decimal` 或者指定字段名来说是有用的：

```scala
val table = tEnv.fromValues(
    DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("name", DataTypes.STRING())
    ),
    row(1, "ABC"),
    row(2L, "ABCDE")
)
```

生成表的 schema 为：

```
root
|-- id: DECIMAL(10, 2)
|-- name: STRING
```

##### 2.1.3、Select

###### 两种模式下都可用

与 SQL 的 `SELECT` 语句类似。

```scala
val orders = tableEnv.from("Orders")
Table result = orders.select($"a", $"c" as "d")
```

可以使用 `*` 作为通配符，来选择表的所有列：

```scala
Table result = orders.select($"*")
```

##### 2.1.4、As

###### 两种模式下都可用

重命名属性。

```java
val orders: Table = tableEnv.from("Orders").as("x", "y", "z", "t")
```

##### 2.1.5、Where / Filter

###### 两种模式下都可用

与 SQL 的 `WHERE` 子句类似。

```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.filter($"a" % 2 === 0)
```

##### 2.2、列操作

##### 2.2.1、AddColumns

###### 两种模式下都可用

增加一个属性，如果增加的属性已经存在，则抛出异常。

```scala
val orders = tableEnv.from("Orders")
val result = orders.addColumns(concat($"c", "Sunny"))
```

##### 2.2.2、AddOrReplaceColumns

###### 两种模式下都可用

增加一个属性。如果增加的字段名称已经存在，则进行替换。另外，如果增加的属性中有重复的属性名，则使用最后一个。

```scala
val orders = tableEnv.from("Orders")
val result = orders.addOrReplaceColumns(concat($"c", "Sunny") as "desc")
```

##### 2.2.3、DropColumns

###### 两种模式下都可用

```scala
val orders = tableEnv.from("Orders")
val result = orders.dropColumns($"b", $"c")
```

##### 2.2.4、RenameColumns

###### 两种模式下都可用

对属性重命名。属性表达式应该是别名表达式，只有存在的属性才能被重命名。

```scala
val orders = tableEnv.from("Orders")
val result = orders.renameColumns($"b" as "b2", $"c" as "c2")
```

##### 2.3、聚合

##### 2.3.1、GroupBy 聚合

###### 两种模式下都可用、结果更新

与 SQL `GROUP BY` 子句类似。

```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.groupBy($"a").select($"a", $"b".sum().as("d"))
```

对于流查询，需要用于计算查询结果的状态可能会无限的增长。请设置一个空闲状态保留时长，以防止状态大小过大。

##### 2.3.2、GroupBy Window 聚合

###### 两种模式下都可用

基于分组窗口和分组键进行分组和聚合

```scala
val orders: Table = tableEnv.from("Orders")
val result: Table = orders
    .window(Tumble over 5.minutes on $"rowtime" as "w") // define window
    .groupBy($"a", $"w") // group by key and window
    .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".sum as "d") // access window properties and aggregate
```

##### 2.3.3、Over Window 聚合

###### 两种模式下都可用

与 SQL 的 `OVER` 子句类似。Over window 聚合是基于一个窗口中前（preceding）、后（succeeding）行，对窗口中的每一行分别计算的。

```scala
val orders: Table = tableEnv.from("Orders")
val result: Table = orders
    // define window
    .window(
        Over
          partitionBy $"a"
          orderBy $"rowtime"
          preceding UNBOUNDED_RANGE
          following CURRENT_RANGE
          as "w")
    .select($"a", $"b".avg over $"w", $"b".max().over($"w"), $"b".min().over($"w")) // sliding aggregate
```

所有的聚合必须都是基于相同的窗口的。目前，仅支持 PRECEDING（UNBOUNDED 和有限的）到 CURRENT ROW 范围的窗口。不支持使用 FOLLOWING 的窗口范围。ORDER BY 必须指定在一个时间属性上。

##### 2.3.4、Distinct 聚合

###### 两种模式下都可用、结果更新

与 SQL 的 DISTINCT 聚合子句，比如 `COUNT(DISTINCT a)` 类似。Distinct 聚合声明聚合函数（内置的或者自定义的）只应用在不同的输入值上。Distinct 可以用于 GroupBy 聚合、GroupBy Window 聚合以及 Over Window 聚合。

```scala
val orders: Table = tableEnv.from("Orders")
// Distinct aggregation on group by
val groupByDistinctResult = orders
    .groupBy($"a")
    .select($"a", $"b".sum.distinct as "d")
// Distinct aggregation on time window group by
val groupByWindowDistinctResult = orders
    .window(Tumble over 5.minutes on $"rowtime" as "w").groupBy($"a", $"w")
    .select($"a", $"b".sum.distinct as "d")
// Distinct aggregation on over window
val result = orders
    .window(Over
        partitionBy $"a"
        orderBy $"rowtime"
        preceding UNBOUNDED_RANGE
        as $"w")
    .select($"a", $"b".avg.distinct over $"w", $"b".max over $"w", $"b".min over $"w")
```

用户自定义的函数也可以使用 `DISTINCT` 修饰符。要对不同值进行聚合，只用在聚合函数后加上 distinct 修饰符即可。

```scala
al orders: Table = tEnv.from("Orders")

// Use distinct aggregation for user-defined aggregate functions
val myUdagg = new MyUdagg()
orders.groupBy($"users").select($"users", myUdagg.distinct($"points") as "myDistinctResult")
```

对于流查询，需要用于计算查询结果的状态可能会无限的增长。请设置一个空闲状态保留时长，以防止状态大小过大。

##### 2.3.5、Distinct

###### 两种模式下都可用、结果更新

与 SQL 的 `DISTINCT` 子句类似，返回具有不同值组合的记录：

```scala
val orders: Table = tableEnv.from("Orders")
val result = orders.distinct()
```

对于流查询，需要用于计算查询结果的状态可能会无限的增长。请设置一个空闲状态保留时长，以防止状态大小过大。

##### 2.4、连接（Joins）

##### 2.4.1、Inner Join

###### 两种模式下都可用

与 SQL JOIN 子句类似。连接两个表。两个表必须有不同的字段名称，必须通过 join 算子或者使用 where 或者 filter 算子定义至少一个的等值连接谓词（equality join predicate）。

```scala
val left = tableEnv.from("MyTable").select($"a", $"b", $"c")
val right = tableEnv.from("MyTable").select($"d", $"e", $"f")
val result = left.join(right).where($"a" === $"d").select($"a", $"b", $"e")
```

对于流查询，需要用于计算查询结果的状态可能会无限的增长。请设置一个空闲状态保留时长，以防止状态大小过大。

##### 2.4.2、Outer Join

###### 两种模式下都可用、结果更新

与 SQL 的 `LEFT/RIGHT/FULL OUTER JOIN` 子句类似。连接两个表。两个表必须有不同的字段名称，必须定义至少一个的等值连接谓词。

```scala
val left = tableEnv.from("MyTable").select($"a", $"b", $"c")
val right = tableEnv.from("MyTable").select($"d", $"e", $"f")

val leftOuterResult = left.leftOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
val rightOuterResult = left.rightOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
val fullOuterResult = left.fullOuterJoin(right, $"a" === $"d").select($"a", $"b", $"e")
```

对于流查询，需要用于计算查询结果的状态可能会无限的增长。请设置一个空闲状态保留时长，以防止状态大小过大。

##### 2.4.3、Interval Join

###### 两种模式下都可用

区间连接是常规连接的子集，可以以流的模式进行处理。

区间连接需要至少一个等值连接谓词和一个限制两侧边界的连接条件。连接条件可以通过两个恰当的区间谓词比如（`<`、`<=`、`>=`、`>`）定义，也可以通过一个比较两个输入表的相同类型的时间属性（即、处理时间或事件事件）的等值谓词定义。

```scala
val left = tableEnv.from("MyTable").select($"a", $"b", $"c", $"ltime")
val right = tableEnv.from("MyTable").select($"d", $"e", $"f", $"rtime")

val result = left.join(right)
  .where($"a" === $"d" && $"ltime" >= $"rtime" - 5.minutes && $"ltime" < $"rtime" + 10.minutes)
  .select($"a", $"b", $"e", $"ltime")
```

##### 2.4.4、和表函数（Table Function，UDTF）进行内连接

###### 两种模式下都可用

使用一个表函数的结果与一个表进行连接。左（外）表的每一行都和它对应的表函数调用产生的所有行进行连接。如果左表某一行表函数调用返回的结果是空的，那么这一行会被删除。

```scala
// instantiate User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .joinLateral(split($"c") as ("s", "t", "v"))
    .select($"a", $"b", $"s", $"t", $"v")
```

##### 2.4.5、和表函数（UDTF）进行左外连接

###### 两种模式下都可用

使用一个表函数的结果与一个表进行连接。左（外）表的每一行都和它对应的表函数调用产生的所有行进行连接。如果左表某一行表函数调用返回的结果是空的，那么改行对应的值会通过 null 值进行补齐。

目前，表函数左外连接的连接谓词只能是空或者是字面量 `true`。

```scala
// instantiate User-Defined Table Function
val split: TableFunction[_] = new MySplitUDTF()

// join
val result: Table = table
    .leftOuterJoinLateral(split($"c") as ("s", "t", "v"))
    .select($"a", $"b", $"s", $"t", $"v")
```

##### 2.4.6、和临时表（Temporal Table）连接

临时表是追踪随着时间变化的表。

临时表函数可以访问某一时间点临时表的状态。一个表和临时表函数连接的语法与和表函数进行内连接的语法相同。

目前，只支持和表函数进行内连接。

```java
Table ratesHistory = tableEnv.from("RatesHistory");

// register temporal table function with a time attribute and primary key
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction(
    "r_proctime",
    "r_currency");
tableEnv.registerFunction("rates", rates);

// join with "Orders" based on the time attribute and key
Table orders = tableEnv.from("Orders");
Table result = orders
    .joinLateral(call("rates", $("o_proctime")), $("o_currency").isEqual($("r_currency")));
```

##### 2.5、集合操作（Set Operations）

##### 2.5.1、Union

###### 批处理模式可用

与 SQL `UNION` 子句类似，合并两个表并去除重复的记录。两个表必须有相同的字段类型。

```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.union(right)
```

##### 2.5.2、UnionAll

###### 两种模式下都可用

与 SQL `UNION ALL` 子句类似。合并两个表，两个表必须有相同类型的字段。

```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.unionAll(right)
```

##### 2.5.3、Intersect

###### 批处理模式可用

与 SQL `INTERSECT` 子句类似。取两个表交集，如果一条记录在两个表中一个或者两个中出现一次以上，结果只会返回一条记录，即，结果会去重。两个表必须有相同类型的字段。

```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.intersect(right)
```

##### 2.5.4、IntersectAll

###### 批处理模式可用

与 SQL `INTERSECT ALL` 子句类似。取两个表交集，如果一条记录在两个表中一个或者两个中出现一次以上，结果中会包含在两个表中出现的相同次数。两个表必须有相同类型的字段。

```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.intersectAll(right)
```

##### 2.5.4、Minus

###### 批处理模式可用

与 SQL `EXCEPT` 子句类似。返回左表中在右表中不存在的记录（差集），如果一条记录在左表中出现一次以上，结果只会出现一次，即，结果会去重。两个表必须有相同类型的字段。

```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.minus(right)
```

##### 2.5.5、MinusAll

###### 批处理模式可用

与 SQL `EXCEPT ALL` 子句类似。返回左表中在右表中不存在的记录（差集），如果一条记录在左表中出现 `n` 次并在右表中出现 `m` 次，结果会出现 `n-m` 次，即，结果会移除有表中出现相同次数的记录。两个表必须有相同类型的字段。

```scala
val left = tableEnv.from("orders1")
val right = tableEnv.from("orders2")

left.minusAll(right)
```

##### 2.5.6、In

###### 两种模式下都可用

与 SQL `IN` 子句类似。如果一个表达式存在于子查询表中返回 true。子查询表必须只有一个字段。这个字段必须与表达式具有相同的数据类型。

```scala
val left = tableEnv.from("Orders1")
val right = tableEnv.from("Orders2")

val result = left.select($"a", $"b", $"c").where($"a".in(right))
```

对于流查询，需要用于计算查询结果的状态可能会无限的增长。请设置一个空闲状态保留时长，以防止状态大小过大。

##### 2.6、OrderBy、Offset 和 Fetch

##### 2.6.1、Order By

###### 两种模式下都可用

与 SQL 的 `ORDER BY` 子句类似。返回跨所有分区全局排序的记录。对于无边界表，这个操作需要基于一个时间属性排序，或者需要一个后续的 fetch 操作。

```scala
val result = tab.orderBy($"a".asc)
```

##### 2.6.2、Offset 和 Fetch

###### 两种模式下都可用

与 SQL `OFFSET` 和 `FETCH` 子句类似。offset 操作获取从某个偏移位置开始的结果（可能是排过序的）。fetch 操作获取前 n 行的结果。通常这两个操作之前会有一个排序算子。对于无边界表，对于 offset 操作 fetch 操作是必需的。

```scala
// returns the first 5 records from the sorted result
val result1: Table = in.orderBy($"a".asc).fetch(5)

// skips the first 3 records and returns all following records from the sorted result
val result2: Table = in.orderBy($"a".asc).offset(3)

// skips the first 10 records and returns the next 5 records from the sorted result
val result3: Table = in.orderBy($"a".asc).offset(10).fetch(5)
```

##### 2.7、Insert

###### 两种模式下都可用

与 SQL 查询中的 `insert into` 子句类似，这个方法执行对一个注册的输出表的插入操作。`insertInto()` 方法会将 `INSERT INTO` 转换为一个 `TablePipeline`。可以使用 `TablePipeline.execute()` 解释，也可以使用 `TablePipeline.execute()` 执行。

必须在 TableEnvironment 中注册输出表。此外，注册的表的 schema 必须与查询的 schema 匹配。

```scala
val orders = tableEnv.from("Orders")
orders.insertInto("OutOrders").execute()
```

##### 2.8、Group 窗口（Group Windows）

group window 聚合，基于时间或者行计数区间（row-count intervals）将行分组为有限的组，并对每个组执行聚合函数。对于批处理表，窗口是按照时间区间进行记录分组的快捷方式。

```scala
val table = input
  .window([w: GroupWindow] as $"w")  // define window with alias w
  .groupBy($"w")   // group the table by window w
  .select($"b".sum)  // aggregate
```

使用 `window(w: GroupedWindow)` 子句定义的窗口需要一个别名，使用 `as` 子句指定。为了按照窗口对一个表进行 group，窗口别名必须在 `groupBy(...)` 子句中被引用。

```scala
val table = input
  .window([w: GroupWindow] as $"w") // define window with alias w
  .groupBy($"w", $"a")  // group the table by attribute a and window w
  .select($"a", $"b".sum)  // aggregate
```

在流环境中，如果除了对窗口 group 外，还对一个或多个属性举行 group，窗口聚合只能并行地执行运算。只引用了一个窗口别名的 `groupBy(...)` 子句，只能由单独一个非并行的任务进行运算。

窗口属性，比如一个时间窗口的 `start`、`end`、或者 rowtime 时间戳可以作为窗口别名的属性添加到 select 语句中，比如 `w.start`、`w.end`、`w.rowtime` 。窗口 start 和 rowtime 时间戳是包含在内的窗口上、下边界。相反的，窗口 end 时间戳是不包含在内的窗口上边界。比如下午两点中开始的 30 分钟的滚动窗口，它的开始（start）时间戳是 `14:00:00.000`，`14:29:59.999` 是它的 rowtime 时间戳，`14:30:00.000` 是它的结束时间戳。

```scala
val table = input
  .window([w: GroupWindow] as $"w")  // define window with alias w
  .groupBy($"w", $"a")  // group the table by attribute a and window w
  .select($"a", $"w".start, $"w".end, $"w".rowtime, $"b".count) // aggregate and add window start, end, and rowtime timestamps
```

`Window` 参数定义了行是如何映射到窗口的。`Window` 不是一个用户可以实现的接口。而且， Table API 提供了一组预定义的具有特殊语义的 `Window` 类。支持的窗口定义如下：

##### 2.8.1、滚动窗口（Tumbling Windows）

滚动窗口将行分配给**不重叠的、连续的、固定长度**的窗口。滚动窗口可以基于事件时间、处理时间、或者行计数（row-count）定义。

使用 `Tumble` 类定义滚动窗口：

| 方法 | 描述                                                         |
| ---- | ------------------------------------------------------------ |
| over | 定义窗口长度，可以是时间或者行计数的区间                     |
| on   | group（时间区间）或 sort（行计数）所基于的时间属性。对于批查询可以是任意 Long 类型或 Timestamp 类型的属性。对于流查询，必须是[声明的事件时间或处理时间属性](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/concepts/time_attributes/) |
| as   | 为窗口分配一个别名。别名用于在后续的 `groupBy()` 子句中引用该窗口，也可以在 select 子句中筛选窗口属性（窗口的 start、end、rowtime 时间戳） |

```scala
// Tumbling Event-time Window
.window(Tumble over 10.minutes on $"rowtime" as $"w")

// Tumbling Processing-time Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.minutes on $"proctime" as $"w")

// Tumbling Row-count Window (assuming a processing-time attribute "proctime")
.window(Tumble over 10.rows on $"proctime" as $"w")
```

##### 2.8.2、滑动窗口（Sliding Windows）

滑动窗口**具有一个固定的大小并通过一个指定滑动区间滑动**。如果滑动区间比窗口大小小，滑动窗口会重叠。这样，相同记录可能会被分配到多个窗口。可以基于事件时间、处理时间、或者行计数（row-count）定义。

使用 `Slide` 类定义滑动窗口：

| 方法  | 描述                                                         |
| ----- | ------------------------------------------------------------ |
| over  | 定义窗口长度，可以是时间或者行计数的区间                     |
| every | 定义滑动区间，可以是时间或者行计数的区间。必须和窗口大小区就类型相同 |
| on    | group（时间区间）或 sort（行计数）所基于的时间属性。对于批查询可以是任意 Long 类型或 Timestamp 类型的属性。对于流查询，必须是[声明的事件时间或处理时间属性](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/concepts/time_attributes/) |
| as    | 为窗口分配一个别名。别名用于在后续的 `groupBy()` 子句中引用该窗口，也可以在 select 子句中筛选窗口属性（窗口的 start、end、rowtime 时间戳） |

```scala
// Sliding Event-time Window
.window(Slide over 10.minutes every 5.minutes on $"rowtime" as $"w")

// Sliding Processing-time window (assuming a processing-time attribute "proctime")
.window(Slide over 10.minutes every 5.minutes on $"proctime" as $"w")

// Sliding Row-count window (assuming a processing-time attribute "proctime")
.window(Slide over 10.rows every 5.rows on $"proctime" as $"w")
```

##### 2.8.3、会话窗口（Session Windows）

会话窗口没有固定的大小，而是通过一个不活跃区间定义边界的，即，如果在定义的时间段没有事件出现，会话窗口会关闭。可以基于事件时间、处理时间定义。

会话窗口通过 `Session` 类定义：

| 方法    | 描述                                                         |
| ------- | ------------------------------------------------------------ |
| withGap | 以时间区间定义两个窗口之间的时间相隔（gap）                  |
| on      | group（时间区间）或 sort（行计数）所基于的时间属性。对于批查询可以是任意 Long 类型或 Timestamp 类型的属性。对于流查询，必须是[声明的事件时间或处理时间属性](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/concepts/time_attributes/) |
| as      | 为窗口分配一个别名。别名用于在后续的 `groupBy()` 子句中引用该窗口，也可以在 select 子句中筛选窗口属性（窗口的 start、end、rowtime 时间戳） |

```scala
// Session Event-time Window
.window(Session withGap 10.minutes on $"rowtime" as $"w")

// Session Processing-time Window (assuming a processing-time attribute "proctime")
.window(Session withGap 10.minutes on $"proctime" as $"w")
```

##### 2.9、Over 窗口（Over Windows）

Over window 聚合来自于标准 SQL（over 子句），是定义在一个查询的 `SELECT` 子句中的。与 group window 聚合不同的是，over window 聚合不会合并行，而是针对每行，在它的相邻行的一定范围上执行聚合。

使用 `window(w: OverWindow*)` 子句定义 over window，并在`select()` 方法中使用别名来引用。

```scala
val table = input
  .window([w: OverWindow] as $"w")              // define over window with alias w
  .select($"a", $"b".sum over $"w", $"c".min over $"w") // aggregate over the over window w
```

`OverWindow` 定义了一个范围的行，在此之上执行聚合运算。`OverWindow` 不是用户可以实现的接口。Table API 提供了 `Over` 类，可以配置 over window 的属性。over window 可以基于事件时间或处理时间，并在指定的时间区间范围或行计数（row-count）上定义。支持的 over window 定义被暴露为基于 `Over` （和其他类）的方法：

##### 2.9.1、Partition By

###### 可选

定义基于一个或多个属性进行分区。每个分区都是各别排序的，聚合函数是分别应用于各个分区的。

注意：在流环境中，如果窗口包含一个 partition by 子句，over window 聚合只能并行地执行。没有 `partitionBy(...)`，流是由单个非并行任务处理的。

##### 2.9.2、Order By

###### 必须

定义每个分区中行的顺序，以及聚合函数应用到行的顺序。

注意：对于流查询，必须是一个声明的事件时间或处理时间属性。目前只支持单一排序属性。

##### 2.9.3、Preceding

###### 可选

定义位于窗口中并在当前行之前的行区间。可以是时间区间或行计数区间。

有边界的 over windows 是用区间大小指定的，比如，10 分钟作为时间区间或 10 行作为行计数区间。

无边界的 over windows 使用一个常数指定的，即，`UNBOUNDED_RANGE` 作为时间区间或 `UNBOUNDED_ROW` 作为行计数区间。无边界 over windows 开始于分区的第一行。

如果没有 preceding 子句，窗口则使用 `UNBOUNDED_RANGE` 和 `CURRENT_RANGE` 作为默认的 preceding 和 following。

##### 2.9.4、Following

###### 可选

定义位于窗口中并在当前行之后的行区间。区间必须和 preceding 区间单位一致（时间或行计数）。

目前，对 following 当前行的 over window 聚合还不支持。但是，可以使用两个常量中的一个来指定：

- `CURRENT_ROW` 将窗口的上界设置为当前行
- `CURRENT_RANGE` 设置窗口上界为当前行的排序键（sort key），即，与当前行具有相同排序键的所有行都包含在窗口内。

如果没有 following 子句，时间区间窗口的上界是 `CURRENT_RANGE`，行计数区间窗口的上界是 `CURRENT_ROW`。

##### 2.9.5、As

###### 必须

为 over 窗口指定别名。别名用于在 `select()` 子句中引用 over 窗口。

注意，目前相同 `select()` 子句中的所有聚合函数必须都是计算同一个 over window 的。

##### 2.9.6、无边界的 over windows

```scala
// Unbounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_RANGE as "w")

// Unbounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_RANGE as "w")

// Unbounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding UNBOUNDED_ROW as "w")
 
// Unbounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding UNBOUNDED_ROW as "w")
```

##### 2.9.7、有边界的 over windows

```scala
// Bounded Event-time over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 1.minutes as "w")

// Bounded Processing-time over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding 1.minutes as "w")

// Bounded Event-time Row-count over window (assuming an event-time attribute "rowtime")
.window(Over partitionBy $"a" orderBy $"rowtime" preceding 10.rows as "w")
  
// Bounded Processing-time Row-count over window (assuming a processing-time attribute "proctime")
.window(Over partitionBy $"a" orderBy $"proctime" preceding 10.rows as "w")
```

##### 2.10、基于行的操作

基于行的操作生成具有多个行的输出。

##### 2.10.1、Map

###### 两种模式都可用

使用用户定义的标量函数（scalar function）或者内置的标量函数执行 map 操作。如果输出类型是组合类型（composite type），输出会被扁平化（flattened）。

```scala
class MyMapFunction extends ScalarFunction {
  def eval(a: String): Row = {
    Row.of(a, "pre-" + a)
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] =
    Types.ROW(Types.STRING, Types.STRING)
}

val func = new MyMapFunction()
val table = input
  .map(func($"c")).as("a", "b")
```

##### 2.10.2、FlatMap

##### 两种模式都可用

使用表函数（table 函数）执行 `flatMap` 操作：

```scala
class MyFlatMapFunction extends TableFunction[Row] {
  def eval(str: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(2)
        row.setField(0, s)
        row.setField(1, s.length)
        collect(row)
      })
    }
  }

  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.INT)
  }
}

val func = new MyFlatMapFunction
val table = input
  .flatMap(func($"c")).as("a", "b")
```

##### 2.10.3、Aggregate

###### 两种模式都可用、结果

使用聚合函数执行聚合操作。必须使用一个 select 语句来关闭“聚合”，并且这个 select 语句不能有聚合。如果输出类型是组合类型，聚合的输出会被扁平化。

```scala
case class MyMinMaxAcc(var min: Int, var max: Int)

class MyMinMax extends AggregateFunction[Row, MyMinMaxAcc] {

  def accumulate(acc: MyMinMaxAcc, value: Int): Unit = {
    if (value < acc.min) {
      acc.min = value
    }
    if (value > acc.max) {
      acc.max = value
    }
  }

  override def createAccumulator(): MyMinMaxAcc = MyMinMaxAcc(0, 0)

  def resetAccumulator(acc: MyMinMaxAcc): Unit = {
    acc.min = 0
    acc.max = 0
  }

  override def getValue(acc: MyMinMaxAcc): Row = {
    Row.of(Integer.valueOf(acc.min), Integer.valueOf(acc.max))
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Types.INT, Types.INT)
  }
}

val myAggFunc = new MyMinMax
val table = input
  .groupBy($"key")
  .aggregate(myAggFunc($"a") as ("x", "y"))
  .select($"key", $"x", $"y")
```

##### 2.10.4、Group Window Aggregate

基于一个 group window 和可能有的一个或者多个分组键进行分组和聚合。必须以 select 语句关闭“聚合”，并且这个 select 语句不能包括“*”和聚合函数。

```scala
val myAggFunc = new MyMinMax
val table = input
    .window(Tumble over 5.minutes on $"rowtime" as "w") // define window
    .groupBy($"key", $"w") // group by key and window
    .aggregate(myAggFunc($"a") as ("x", "y"))
    .select($"key", $"x", $"y", $"w".start, $"w".end) // access window properties and aggregate results
```

##### 2.10.5、FlatAggregate

与 GroupBy 聚合类似。基于分组键使用后续的**表聚合算子**（table aggregation operator）按组对行进行聚合。与 AggregateFunction 不同的是，TableAggregateFunction 可能会为一个分组返回 0 条或者多条记录。必须使用 select 语句来关闭“flatAggregate”，并且这个 select 语句不能包含聚合函数。

除了使用 `emitValue` 来输出结果，还可以使用 `emitUpdateWithRetract` 方法。不同于 `emitValue`，`emitUpdateWithRetract` 是用于输出被更新的值的。这个方法用回撤模式（retract mode）增量地输出数据，即，一旦有更新，必须在输出新的更新后的数据之前撤回旧的数据。如果在表聚和函数中都定义了这两个方法，会优先使用 `emitUpdateWithRetract` 方法，因为，`emitUpdateWithRetract` 是增量地输出值的，它被认为是更高效的。

```scala
import java.lang.{Integer => JInteger}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction

/**
 * Accumulator for top2.
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
}

/**
 * The top2 user-defined table aggregate function.
 */
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top2Accum, its: JIterable[Top2Accum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val top2 = iter.next()
      accumulate(acc, top2.first)
      accumulate(acc, top2.second)
    }
  }

  def emitValue(acc: Top2Accum, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
    // emit the value and rank
    if (acc.first != Int.MinValue) {
      out.collect(JTuple2.of(acc.first, 1))
    }
    if (acc.second != Int.MinValue) {
      out.collect(JTuple2.of(acc.second, 2))
    }
  }
}

val top2 = new Top2
val orders: Table = tableEnv.from("Orders")
val result = orders
    .groupBy($"key")
    .flatAggregate(top2($"a") as ($"v", $"rank"))
    .select($"key", $"v", $"rank")
```

对于流查询，需要用于计算查询结果的状态可能会无限的增长。请设置一个空闲状态保留时长，以防止状态大小过大。

#### 3、数据类型

请移步[数据类型](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/types/)。

泛型（generic types）和（嵌套的）组合类型（比如，POJOs，元组，rows，Scala case classes）也都可以是一个行的属性。

具有任意嵌套的组合类型的属性可以通过[值访问函数（value access functions）](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/functions/systemfunctions/#value-access-functions)访问。

泛型类型被当作是黑盒的，可以通过[用户自定义函数](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/functions/udfs/)传递或者处理。


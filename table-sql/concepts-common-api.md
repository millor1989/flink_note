### 概念和通用 API

Table API 和 SQL 集成于一个联合 API。这个 API 的核心概念是 `Table`——作为查询的输入和输出来提供服务。

#### 1、Table API 和 SQL 程序的结构

```scala
import org.apache.flink.table.api._
import org.apache.flink.connector.datagen.table.DataGenOptions

// 为批执行或者流执行创建一个 TableEnvironment.
val tableEnv = TableEnvironment.create(/*…*/)

// 创建 source table
tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
  .schema(Schema.newBuilder()
    .column("f0", DataTypes.STRING())
    .build())
  .option(DataGenOptions.ROWS_PER_SECOND, 100)
  .build())

// 创建 sink table (using SQL DDL)
tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable");

// 通过 Table API 查询创建一个 Table 对象
val table1 = tableEnv.from("SourceTable");

// 通过 SQL 查询创建一个 Table 对象
val table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable");

// 将一个 Table API 结果 Table 发送到 TableSink, SQL 结果与此相同
val tableResult = table1.executeInsert("SinkTable");
```

另外 Table API 和 SQL 查询可以很容易地集成和嵌入到 DataStream 程序中。

#### 2、创建 `TableEnvironment`

`TableEnvironment` 是 Table API 和 SQL 集成的入口，它负责：

- 在内部 catalog 中注册一个 `Table`
- 注册 catalogs
- 加载可插拔模块
- 执行 SQL 查询
- 注册用户自定义的（扩展、表、聚合）函数
- `DataStream` 和 `Table`（`StreamTableEnvironment` 的情况下）之间的转换

`Table` 总是和特定的 `TableEnvironment` 绑定的。在相同的查询中不能对不同 TableEnvironments 的表进行结合操作（比如，join、union）。通过调用 `TableEnvironment.create()` 方法来创建 `TableEnvironment`：

```scala
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

val settings = EnvironmentSettings
    .newInstance()
    .inStreamingMode()
    //.inBatchMode()
    .build()

val tEnv = TableEnvironment.create(settings)
```

另外，用户可以从既存的 `StreamExecutionEnvironment` 来创建 `StreamTableEnvironment` 来与 `DataStream` API 进行互操作。

```scala
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)
```

#### 3、在 Catalog 中创建表

`TableEnvironment` 维护着一个表的 catalogs 的 map，它是用一个标识符（identifier）创建的。每个标识符由三部分组成：catalog 名称、数据库名称、对象名称。如果没有指定 catalog 或数据库，则会使用当前默认的值。

表可以是虚拟的（视图）也可以是常规的（表 `TABLES`）。可以从存在的 `Table` 对象创建视图，视图通常是 Table API 或 SQL 查询的结果。表 `TABLES` 描述外部数据，比如文件、数据库表、或者消息队列。

##### 3.1、临时表和永久表（Temporary vs Permanent tables）

表可以是临时的——绑定到单个 Flink session 的生命周期；也可以是永久的——对于多个 Flink sessions 和集群可见。

**永久表**需要一个 catalog（比如 Hive Metastore）来维护表的元数据（metadata）。一旦创建了永久表，它对任意连接到这个 catalog 的 Flink session 都是可见的，并且在这个表被明确地删除之前会一直存在。

**临时表**总是保存在内存中，并且仅存在于创建它们的 Flink session 的周期中。这些表对其它 session 不可见。它们没有被绑定到任何 catalog 或数据库，但是可以在 catalog 或数据库的命名空间中创建。临时表对应的数据库被删除时，临时表也不会被删除。

##### 3.1.1、遮蔽（Shadowing）

可以用与既存的永久表相同的标识符来创建一个临时表。临时表会遮蔽（shadow）对应的永久表——只要临时表存在对应的永久表就不可访问。针对相应标识符的所有查询都会在临时表上执行。

这对于试验很有用——可以首先对临时表（比如，只是全部数据的一个子集，或者数据是混淆的）进行相同的查询。证实查询正确之后，可以在真实的生产环境表上运行查询。

##### 3.2、创建一个表

##### 3.2.1、虚拟表（Virtual Tables）

一个 `Table` API 对象对应于一个 SQL 数据中的 `VIEW`（虚拟表）。它封装了一个逻辑查询计划（logical query plan）。可以在 catalog 中按照如下方式创建：

```scala
// 获取 TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// table is the result of a simple projection query 
val projTable: Table = tableEnv.from("X").select(...)

// 将 Table projTable 注册为 table "projectedTable"
tableEnv.createTemporaryView("projectedTable", projTable)
```

**注意**：`Table` 对象与关系型数据库系统中的 `VIEW` 类似，即，定义 `Table` 的查询没有被优化，但是当另一个查询引用注册的 `Table` 时它就会被内联（inlined）。如果多个查询引用了相同的注册的 `Table`，它就会被每个引用查询内联并执行多次，即，注册的 `Table` 的结果不会被共享。

##### 3.2.2、连机器表（Connector Tables）

也可以从连接器（connector）声明中创建关系数据库中已知的表（TABLE）。连接器描述了存储表数据的外部系统。存储系统，像 Apache Kafka 或者常规的文件系统都可以用连接器进行声明。

可以直接使用 Table API 创建这种表，也可以使用 SQL DDL。

```scala
// Using table descriptors
final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
    .schema(
        Schema.newBuilder()
    		.column("f0", DataTypes.STRING())
    		.build()
    )
    .option(DataGenOptions.ROWS_PER_SECOND, 100)
    .build();

tableEnv.createTable("SourceTableA", sourceDescriptor);
tableEnv.createTemporaryTable("SourceTableB", sourceDescriptor);

// Using SQL DDL
tableEnv.executeSql("CREATE [TEMPORARY] TABLE MyTable (...) WITH (...)")
```

##### 3.3、扩展表标识符（Expanding Table identifiers）

表标识符包括 catalog、数据库和表名三部分。

用户可以设置一个 catalog 和 catalog 中的一个数据库为“当前 catalog” 和“当前数据库”。这样，catalog、数据库两部分标识符就是可选的——没有提供时默认使用它们。用户可以通过 table API 和 SQL 切换当前 catalog 和当前数据库。

标识符遵循 SQL 要求意味着可以使用反引号（\`）进行转义。

```scala
// get a TableEnvironment
val tableEnv: TableEnvironment = ...;
tableEnv.useCatalog("custom_catalog")
tableEnv.useDatabase("custom_database")

val table: Table = ...;

// register the view named 'exampleView' in the catalog named 'custom_catalog'
// in the database named 'custom_database' 
tableEnv.createTemporaryView("exampleView", table)

// register the view named 'exampleView' in the catalog named 'custom_catalog'
// in the database named 'other_database' 
tableEnv.createTemporaryView("other_database.exampleView", table)

// register the view named 'example.View' in the catalog named 'custom_catalog'
// in the database named 'custom_database' 
tableEnv.createTemporaryView("`example.View`", table)

// register the view named 'exampleView' in the catalog named 'other_catalog'
// in the database named 'other_database' 
tableEnv.createTemporaryView("other_catalog.other_database.exampleView", table)
```

#### 4、查询表

##### 4.1、Table API

Table API 是用于 Scala 和 Java 的语言集成查询 API。与 SQL 相比，查询不是以字符串指定的，而是用宿主语言（host language）一步步构成的。

此 API 是基于 `Table` 类的，`Table` 类代表一个表（流或批）并提供了实现关系型操作的方法，这些方法返回一个新的 `Table` 对象——表示对输入 `Table` 执行了关系型操作后的结果。某些关系型操作是由多个方法调用构成的，比如 `table.groupBy(...).select()`。

一个简单的 Table API 聚合查询例子：

```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register Orders table

// scan registered Orders table
val orders = tableEnv.from("Orders")
// compute revenue for all customers from France
val revenue = orders
  .filter($"cCountry" === "FRANCE")
  .groupBy($"cID", $"cName")
  .select($"cID", $"cName", $"revenue".sum AS "revSum")

// emit or convert Table
// execute query
```

**注意**：Scala Table API 使用了以美元符号（\$）为开始的 Scala 字符串互操作来易用 `Table` 的属性。Table API 使用了 Scala implicits。需要 `import`：

- `org.apache.flink.table.api._`：隐式的表达式转换
- `org.apache.flink.api.scala._` 和 `org.apache.flink.table.api.brigde.scala._`：与 DataStream 之间的相互转换。

##### 4.2、SQL

Flink 的 SQL 集成是基于实现了 SQL 标准的 Apache Calcite 的。SQL 查询通过常规的字符串来指定。

如下例子展示如何指定一个查询并将结果返回为一个 `Table`:

```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register Orders table

// compute revenue for all customers from France
val revenue = tableEnv.sqlQuery("""
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)

// emit or convert Table
// execute query
```

如下例子展示如何指定一个更新查询——将结果插入到注册的表中：

```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// register "Orders" table
// register "RevenueFrance" output table

// compute revenue for all customers from France and emit to "RevenueFrance"
tableEnv.executeSql("""
  |INSERT INTO RevenueFrance
  |SELECT cID, cName, SUM(revenue) AS revSum
  |FROM Orders
  |WHERE cCountry = 'FRANCE'
  |GROUP BY cID, cName
  """.stripMargin)
```

##### 4.3、Table API 和 SQL 结合

Table API 和 SQL 查询可以很容易地结合，因为它们都是返回 `Table` 对象的：

- 可以在 SQL 查询返回的 `Table` 对象上定义 Table API 查询
- 通过将 Table API 的结果注册为 `TableEnvironment` 中的表，SQL 查询在 `FROM` 子句中引用注册的表，可以实现 SQL查询对 Table API 结果的查询。

#### 5、输出一个表

将 `Table` 写到 `TableSink` 来输出 `Table`。`TableSink` 是一个一般接口，支持多种文件格式（比如，CSV、Apache Parquet、Apache Avro）、存储系统（比如，JDBC、Apache HBase、Apache Cassandra、ElasticSearch）、或者消息系统（比如，Apache Kafka、RabbitMQ）。

批处理 `Table` 只能被写到 `BatchTableSink`，而流处理 `Table` 可以写到 `AppendStreamTableSink`、或者 `RetractStreamTableSink`、或者 `UpsertStreamTableSink`。

`Table.executeInsert(String tableName)` 方法将 `Table` 写到注册的 `TableSink`。这个方法通过名称从 catalog 中查找 `TableSink`，并验证 `Table` 和 `TableSink` 的 schema 是否一致。

输出 `Table` 的例子：

```scala
// get a TableEnvironment
val tableEnv = ... // see "Create a TableEnvironment" section

// create an output Table
val schema = Schema.newBuilder()
    .column("a", DataTypes.INT())
    .column("b", DataTypes.STRING())
    .column("c", DataTypes.BIGINT())
    .build()

tableEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/path/to/file")
    .format(FormatDescriptor.forFormat("csv")
        .option("field-delimiter", "|")
        .build())
    .build())

// compute a result Table using Table API operators and/or SQL queries
val result: Table = ...

// emit the result Table to the registered TableSink
result.executeInsert("CsvSinkTable")
```

#### 6、查询的编译和执行（Translate and Execute a Query）

无论输入是流还是批 Table API 和 SQL 查询都被编译为 DataStream 程序。在内部查询是被表示为逻辑查询计划（logical query plan）的，通过两个阶段进行编译：

1. 逻辑计划优化
2. 编译为 DataStream 程序

Table API 或者 SQL 查询编译的时机：

- 调用 `TableEnvironment.executeSql()` 时。该方法用于执行指定的语句，一旦调用这个方法，sql 查询会被马上编译。
- 调用 `Table.executeInsert()` 时。该方法用于将表内容插入到指定的汇路径（sink path），一旦调用这个方法，Table API 会被马上编译。
- 调用 `Table.execute()` 时。这个方法用于将表内容收集到本地客户端，一旦调用这个方法，Table API 会被马上编译。
- 调用 `StatementSet.execute()` 时。一个（通过 `StatementSet.addInsert()` 发送到汇 sink 的）`Table`或者一个（通过 `StatementSet.addInsertSql()` 指定的） INSERT 语句会首先被缓存到 `StatementSet`。一旦调用了 `StatementSet.execute()` 方法，它们就会被编译。所有的汇（sinks）都会被优化到一个 DAG 中。
- 当 `Table` 被转换为 `DataStream` 时，它会被编译。一旦被编译，它就是一个常规的 DataStream 程序，并且在调用 `StreamExecutionEnvironment.execut()` 时执行。

#### 7、查询优化

Apache Flink 利用和扩展 Apache Calcite 来执行复杂的查询优化。这包含了一系列的规则和基于成本的（cost-based）优化，比如：

- 基于 Apache Calcite 进行子查询去相关（decorrelation）
- 投影（*Project*，SQL 属于，大概就是 select）修剪
- 分区（Partition）修剪
- 筛选（Filter）下推（push-down）
- 子计划（sub-plan）去重以避免重复运算
- 特殊子查询重写，包括两部分：
  - 将 IN 和 EXISTS 转换为 left semi join
  - 将 NOT IN 和 NOT EXISTS 转换为 left anti join
- 可选择的 join 重新排序
  - 通过 `table.optimizer.join-order-enabled` 来开启

**Note:** IN/EXISTS/NOT IN/NOT EXISTS are currently only supported in conjunctive conditions in subquery rewriting.

优化器除了基于计划（plan）进行优化外，还基于来自数据源的可用的丰富的统计数据以及每个算子的细粒度的（fine-grain）开销（比如 io，cpu，网络，内存）来进行优化。

高级用户可以通过 `CalciteConfig` 对象来提供自定义的优化，通过调用 `TableEnvironment#getConfig#setPlannerConfig` 来讲 `CalciteConfig` 对象提供给表环境。

#### 8、分析表（Explaning a Table）

Table API 提供了分析用来计算 `Table` 的逻辑查询计划和优化的查询计划的机制。通过 `Table.explain()` 或 `StatementSet.explain()` 方法来进行。`Table.explain()` 返回一个 `Table` 的计划。`StatementSet.explain()` 返回多个汇的计划。它返回描述如下三种计划的字符串：

1. 关系型查询的抽象语法树（Abstract Syntax Tree），即未经优化的逻辑查询计划
2. 优化过的逻辑查询计划
3. 物理执行计划

`TableEnvironment.explainSql()` 和 `TableEnvironment.executeSql()` 支持执行 `EXPLAIN` 语句来获取计划。

如下为使用 `Table.explian()` 分析指定 `Table` 的例子：

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

val table1 = env.fromElements((1, "hello")).toTable(tEnv, $"count", $"word")
val table2 = env.fromElements((1, "hello")).toTable(tEnv, $"count", $"word")
val table = table1
  .where($"word".like("F%"))
  .unionAll(table2)

println(table.explain())
```

其对应的分析结果为：

```
== Abstract Syntax Tree ==
LogicalUnion(all=[true])
:- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
:  +- LogicalTableScan(table=[[Unregistered_DataStream_1]])
+- LogicalTableScan(table=[[Unregistered_DataStream_2]])

== Optimized Physical Plan ==
Union(all=[true], union=[count, word])
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
:  +- DataStreamScan(table=[[Unregistered_DataStream_1]], fields=[count, word])
+- DataStreamScan(table=[[Unregistered_DataStream_2]], fields=[count, word])

== Optimized Execution Plan ==
Union(all=[true], union=[count, word])
:- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
:  +- DataStreamScan(table=[[Unregistered_DataStream_1]], fields=[count, word])
+- DataStreamScan(table=[[Unregistered_DataStream_2]], fields=[count, word])
```

使用 `StatementSet.explain()` 分析有多个汇的计划：

```scala
val settings = EnvironmentSettings.inStreamingMode()
val tEnv = TableEnvironment.create(settings)

val schema = Schema.newBuilder()
    .column("count", DataTypes.INT())
    .column("word", DataTypes.STRING())
    .build()

tEnv.createTemporaryTable("MySource1", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/source/path1")
    .format("csv")
    .build())
tEnv.createTemporaryTable("MySource2", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/source/path2")
    .format("csv")
    .build())
tEnv.createTemporaryTable("MySink1", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/sink/path1")
    .format("csv")
    .build())
tEnv.createTemporaryTable("MySink2", TableDescriptor.forConnector("filesystem")
    .schema(schema)
    .option("path", "/sink/path2")
    .format("csv")
    .build())
    
val stmtSet = tEnv.createStatementSet()

val table1 = tEnv.from("MySource1").where($"word".like("F%"))
stmtSet.addInsert("MySink1", table1)

val table2 = table1.unionAll(tEnv.from("MySource2"))
stmtSet.addInsert("MySink2", table2)

val explanation = stmtSet.explain()
println(explanation)
```

对应的分析结果：

```
== Abstract Syntax Tree ==
LogicalLegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
+- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])

LogicalLegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
+- LogicalUnion(all=[true])
   :- LogicalFilter(condition=[LIKE($1, _UTF-16LE'F%')])
   :  +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])
   +- LogicalTableScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]])

== Optimized Physical Plan ==
LegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
+- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

LegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
+- Union(all=[true], union=[count, word])
   :- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])
   :  +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

== Optimized Execution Plan ==
Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'F%')])(reuse_id=[1])
+- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])

LegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
+- Reused(reference_id=[1])

LegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
+- Union(all=[true], union=[count, word])
   :- Reused(reference_id=[1])
   +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])
```


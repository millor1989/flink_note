### 集成 DataStream API

定义数据处理管道时，Table API 和 DataStream API 是同等重要的。

DataStream API 用相对地低层次命令式（low-level imperative）编程 API 的提供了流处理原语（primitives，即 time、state 和 dataflow 管理）。Table API 抽象了许多内部结构，并提供了结构化和声明式的 API。

两种 APIs 都可以用于有限流和无限流。

处理历史数据的时候需要管理有限流。无限流出现在实时处理场景中，可能需要首先用历史数据进行初始化。

为了高效执行，两种 APIs 都能以优化的批执行模式（optimized batch execution mode）处理有限流。但是，由于批处理只是流的一种特殊情形，所以也可以按照常规的流执行模式（regular streaming execution mode）运行有限流的管道。

使用其中一种 API 的管道可以在不依赖另一种 API 的情况下实现端到端管道。但是，混用两种 APIs 有时是有用的：

- 在使用 DataStream API 实现主要的管道之前，使用表生态系统可以很容易地访问 catalogs 或者连接到外部系统。
- 在使用 DataStream API 实现主要的管道之前，用 SQL 函数进行无状态数据的（stateless data）标准化（normalization）和净化（cleansing）。
- 如果 Table API没有提供某些低层级的操作（比如，自定义 timer 处理）就可以时不时地切换到 DataStream API。

Flink 提供了桥接功能来使得对 DataStream API 的集成尽可能的平滑。

###### DataStream API 和 Table API 之间的切换会增加一些转换开销。比如，部分地运行于二进制数据上的表运行时（table runtime）内部数据结构（即，`rowData`）需要转换为更加用户友好地数据结构（即，`Row`）。通常，这些开销是可以忽略的。

#### 1、DataStream 和 Table 之间的转换

Flink 提供了一个特殊的 `StreamTableEnvironment` 来用于和 DataStream API 集成。这些环境继承了常规的 `TableEnvironment` 同时附带了将 DataStream API 中使用的 `StreamExecutionEnvironment` 作为一个参数的额外方法。

如下为两种 APIs 间相互切换的代码。字段名称和 `Table` 类型是从 `DataStream` 的 `TypeInformation` 自动获取的。由于 DataStream API 本身不支持 changelog 处理，该代码在进行流到表（stream-to-table）和表到流（table-to-stream）转换时，假定了仅追加（append-only）/仅插入（insert-only）语义。

```scala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

// create environments of both APIs
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// create a DataStream
val dataStream = env.fromElements("Alice", "Bob", "John")

// interpret the insert-only DataStream as a Table
val inputTable = tableEnv.fromDataStream(dataStream)

// register the Table object as a view and query it
tableEnv.createTemporaryView("InputTable", inputTable)
val resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable")

// interpret the insert-only Table as a DataStream again
val resultStream = tableEnv.toDataStream(resultTable)

// add a printing sink and execute in DataStream API
resultStream.print()
env.execute()

// prints:
// +I[Alice]
// +I[Bob]
// +I[John]
```

根据查询类型，很多情况下，在将 `Table` 转换为 `DataStream`时，结果动态表是一个不仅产生仅插入（insert-only）变化的管道，还有可能是产生撤回（retractions）和其他类型的更新的管道。

上面的例子展示的是——按照，对于每条输入记录持续地输出按行更新的模式，增量的计算最终结果。但是，对于输入流失有边界的情况，使用批处理原则能够更高效地计算出结果。

在批处理模式下，算子可以在连续的阶段中执行，这些阶段在发出结果之前会消费整个输入表。比如，join 算子在执行实际的 joining（即 sort-merge join 算法）之前会对两个有边界的输入进行排序，或者在消费一个输入之前从另一个表构建一个 hash table（即 hash join 算法的 build/probe 阶段）。

DataStream API 和 Table API 都提供了特殊的 *批运行时模式（batch runtime mode）*。

如下例子，稍加修改，通过判断一个开关标识，可以使用统一的管道处理批或者流数据。

```scala
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

// setup DataStream API
val env = StreamExecutionEnvironment.getExecutionEnvironment()

// set the batch runtime mode
env.setRuntimeMode(RuntimeExecutionMode.BATCH)

// uncomment this for streaming mode
// env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

// setup Table API
// the table environment adopts the runtime mode during initialization
val tableEnv = StreamTableEnvironment.create(env)

// define the same pipeline as above

// prints in BATCH mode:
// +I[Bob, 10]
// +I[Alice, 112]

// prints in STREAMING mode:
// +I[Alice, 12]
// +I[Bob, 10]
// -U[Alice, 12]
// +U[Alice, 112]
```

如果将 changelog 输出到外部系统（即，一个 key-value 存储），就可以看到两种模式都能够产生相同的输出表。通过在输出结果之前消费所有的输入数据，批模式的 changelog 是仅仅由 insert-only 变化构成的。

##### 1.1、依赖和导入

组合了 Table API 和 DataStream API 的项目需要加入一个如下的桥接模块。它包含了往 `flink-table-api-scala`  过渡的依赖和对应的语言专用（language-specific）DataStream API 模块。

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge_2.11</artifactId>
  <version>1.14.4</version>
  <scope>provided</scope>
</dependency>
```

如下导入项是必须的，用来声明基础管道（common pipelines）使用的是 DataStream API 还是 Table API。

```scala
// imports for Scala DataStream API
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._

// imports for Table API with bridging to Scala DataStream API
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
```

##### 1.2、配置

`TableEnvironment` 会采用传递的 `StreamExecutionEnvironment` 中的所有的配置选项。但是，初始化完成后，不能保证 `StreamExecutionEnvironment` 之后的配置变化能够传导到 `StreamTableEnvironment`。Table API 配置到 DataStream API 配置的传导发生在计划生成（planning）阶段。在编写业务逻辑之前，设置好配置项。

```scala
import java.time.ZoneId
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.table.api.bridge.scala._

// create Scala DataStream API

val env = StreamExecutionEnvironment.getExecutionEnvironment

// set various configuration early

env.setMaxParallelism(256)

env.getConfig.addDefaultKryoSerializer(classOf[MyCustomType], classOf[CustomKryoSerializer])

env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

// then switch to Scala Table API

val tableEnv = StreamTableEnvironment.create(env)

// set configuration early

tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

// start defining your pipelines in both APIs...
```

##### 1.3、执行行为

两种 APIs 都提供了执行管道的方法。换句话说，如果执行了这类请求，就会编译一个作业图（job graph）提交到集群并触发执行。结果会流到声明的汇。

通常，两种 APIs 都在方法名中用单词 `execute` 标记了这种行为。但是，Table API 和 DataStream API 的执行行为稍微有些不同。

##### 1.3.1、DataStream API

DataStream API 的 `StreamExecutionEnvironment` 使用建造者模式（builder pattern）构建复杂的管道。管道可能会分成多个分支，分支的终点可能有也可能没有汇。environment 会将所有这些定义的分支缓存，直到作业提交。

`StreamExecutionEnvironment.execute()` 提交整个构建的管道，并在之后清空建造器（builder）。换句话说，不会再有源和汇的声明，新的管道可以被添加到建造器。这样，每个 DataStream 程序通常以 `StreamExecutionEnvironment.execute()` 的调用来终结。或者，`DataStream.executeAndCollect()` 隐式地定义一个汇以将结果流向本地客户端。

##### 1.3.2、Table API

在 Table API 中，只有在 `StatementSet` 中才支持分支管道，并且每个分支管道必须声明一个最终的汇。`TableEnvironment` 和 `StreamTableEnvironment` 都不提供专用的 `execute()` 方法。但是，他们提供了用于将一个源到汇管道或者一个 statement 集合提交的方法。

```java
// execute with explicit sink
tableEnv.from("InputTable").executeInsert("OutputTable")

tableEnv.executeSql("INSERT INTO OutputTable SELECT * FROM InputTable")

tableEnv.createStatementSet()
    .addInsert("OutputTable", tableEnv.from("InputTable"))
    .addInsert("OutputTable2", tableEnv.from("InputTable"))
    .execute()

tableEnv.createStatementSet()
    .addInsertSql("INSERT INTO OutputTable SELECT * FROM InputTable")
    .addInsertSql("INSERT INTO OutputTable2 SELECT * FROM InputTable")
    .execute()

// execute with implicit local sink

tableEnv.from("InputTable").execute().print()

tableEnv.executeSql("SELECT * FROM InputTable").print()
```

为了组合两种执行行为，每次调用 `StreamTableEnvironment.toDataStream` 或者 `StreamTableEnvironment.toChangelogStream` 都会物化（materialize，即 compile）Table API 子管道（sub-pipeline）并将其插入到 DataStream API 管道建造器。这意味着 `StreamExecutionEnvironment.execute()` 或者 `DataStream.executeAndCollect` 必须在其后调用。在 Table API 中的执行行为不会触发这些“外部部分”（"external parts"）。

```java
// (1)

// adds a branch with a printing sink to the StreamExecutionEnvironment
tableEnv.toDataStream(table).print()

// (2)

// executes a Table API end-to-end pipeline as a Flink job and prints locally,
// thus (1) has still not been executed
table.execute().print()

// executes the DataStream API pipeline with the sink defined in (1) as a
// Flink job, (2) was already running before
env.execute()
```

#### 2、批运行时模式（Batch Runtime Mode）

**批运行时模式**是*有限流（bounded）* Flink 程序的一个特殊的执行模式。

一般来说，*有限* 是数据源的一个属性，用来说明在执行前来自该数据源的所有数据是已知的，或者是否会出现新的数据。反过来，如果作业的所有数据源都是有限的，那么该作业就是有限的，否则就是无限的。

**流运行时模式**（Streaming runtime mode），既可以用于有限作业，也可以用于无限作业。

Table API 和 SQL 计划器（planner）分别为两种执行模式提供了一系列特殊的优化器规则和运行时算子。

目前，运行时模式不是从数据源自动获取的，因此，必须明确地设置或者在初始化 `StreamTableEnvironment` 时从 `StreamExecutionEnvironment` 获取。

```scala
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.EnvironmentSettings

// adopt mode from StreamExecutionEnvironment
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setRuntimeMode(RuntimeExecutionMode.BATCH)
val tableEnv = StreamTableEnvironment.create(env)

// or

// set mode explicitly for StreamTableEnvironment
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode)
```

设置运行时模式为 `BATCH` 时，必须满足以下的前提条件：

- 所有的数据源必须声明为有限的。
- 目前，表数据源必须发出 insert-only 的变化。
- 算子需要充足的非堆内存（off-heap memory）用于排序（sorting）和其它中间结果。
- 所有的表操作必须是在批处理模式下可以使用的。目前，某些表操作仅在流模式下可用。

批执行有如下影响（其中包括）：

- 算子既不产生也不使用渐进式的水印。但是，数据源关闭之前会发出一个最大值（maximum）水印
- 根据 `execution.batch-shuffle-mode` tasks 之间的交换可能会被阻塞。这也意味着与流模式相比执行相同的管道潜在需要的资源更少。
- 检查点是无效的。会插入人工（Artifcial）状态后端。
- 表操作不产生增量的更新，而是只产生一个最后转换为 insert-only changelog 流的完整的最终结果。

由于批处理可以被认为是流处理的一种特殊情形，推荐首先实现流处理管道——因为它是有限数据和无限数据的最一般的实现。

理论上，流管道可以执行所有的算子。但是，事实上，某些算子可能不行——因为它们会导致不断增长的（ever-growing）状态，因此不被支持。全局排序是一个仅批处理模式可用的算子的例子。简而言之：应该可以以批模式运行一个可执行的流管道，但是反之则不一定行。

如下为使用 DataGen 表数据源批模式运行的例子。许多数据源都提供了隐式地市 connector 成为有限的的选项，比如，定义一个终结偏移或者时间戳。这个例子中，使用 `number-of-rows` 选项限制了行数。

```scala
import org.apache.flink.api.scala._
import org.apache.flink.table.api._

val table =
    tableEnv.from(
        TableDescriptor.forConnector("datagen")
            .option("number-of-rows", "10") // make the source bounded
            .schema(
                Schema.newBuilder()
                    .column("uid", DataTypes.TINYINT())
                    .column("payload", DataTypes.STRING())
                    .build())
            .build());

// convert the Table to a DataStream and further transform the pipeline
tableEnv.toDataStream(table)
    .keyBy(r => r.getFieldAs[Byte]("uid"))
    .map(r => "My custom operator: " + r.getFieldAs[String]("payload"))
    .executeAndCollect()
    .foreach(println)

// prints:
// My custom operator: 9660912d30a43c7b035e15bd...
// My custom operator: 29f5f706d2144f4a4f9f52a0...
// ...
```

##### 2.1、统一的 Changelog（Changelog Unification）

大多数情况下，当在流和批两种模式之间来回切换时，Table API 和 DataStream API 中的管道定义本身是不用变的。但是，如同之前提到的，由于对批处理模式下的增量操作的避免，结果 changelog 流可能不同。

依赖于事件时间并且利用水印作为完整性标记的基于时间的操作能够生成独立于运行时模式的 insert-only changelog 流。

如下的 Java 例子表明，Flink 程序不仅在 API 层面是统一的，在结果 changelog 流层面也是统一的。这里例子使用一个 join 连接了两个 SQL 表（`UserTable` 和 `OrderTable`）。使用 DataStream API 自定义了一个操作符——用 `KeyedProcessFunction` 和 value state 来去重用户名。

```java
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

// setup DataStream API
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// use BATCH or STREAMING mode
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

// setup Table API
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a user stream
DataStream<Row> userStream = env
    .fromElements(
        Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
        Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
        Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "Bob"))
    .returns(
        Types.ROW_NAMED(
            new String[] {"ts", "uid", "name"},
            Types.LOCAL_DATE_TIME, Types.INT, Types.STRING));

// create an order stream
DataStream<Row> orderStream = env
    .fromElements(
        Row.of(LocalDateTime.parse("2021-08-21T13:02:00"), 1, 122),
        Row.of(LocalDateTime.parse("2021-08-21T13:07:00"), 2, 239),
        Row.of(LocalDateTime.parse("2021-08-21T13:11:00"), 2, 999))
    .returns(
        Types.ROW_NAMED(
            new String[] {"ts", "uid", "amount"},
            Types.LOCAL_DATE_TIME, Types.INT, Types.INT));

// create corresponding tables
tableEnv.createTemporaryView(
    "UserTable",
    userStream,
    Schema.newBuilder()
        .column("ts", DataTypes.TIMESTAMP(3))
        .column("uid", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .watermark("ts", "ts - INTERVAL '1' SECOND")
        .build());

tableEnv.createTemporaryView(
    "OrderTable",
    orderStream,
    Schema.newBuilder()
        .column("ts", DataTypes.TIMESTAMP(3))
        .column("uid", DataTypes.INT())
        .column("amount", DataTypes.INT())
        .watermark("ts", "ts - INTERVAL '1' SECOND")
        .build());

// perform interval join
Table joinedTable =
    tableEnv.sqlQuery(
        "SELECT U.name, O.amount " +
        "FROM UserTable U, OrderTable O " +
        "WHERE U.uid = O.uid AND O.ts BETWEEN U.ts AND U.ts + INTERVAL '5' MINUTES");

DataStream<Row> joinedStream = tableEnv.toDataStream(joinedTable);

joinedStream.print();

// implement a custom operator using ProcessFunction and value state
joinedStream
    .keyBy(r -> r.<String>getFieldAs("name"))
    .process(
        new KeyedProcessFunction<String, Row, String>() {

          ValueState<String> seen;

          @Override
          public void open(Configuration parameters) {
              seen = getRuntimeContext().getState(
                  new ValueStateDescriptor<>("seen", String.class));
          }

          @Override
          public void processElement(Row row, Context ctx, Collector<String> out)
                  throws Exception {
              String name = row.getFieldAs("name");
              if (seen.value() == null) {
                  seen.update(name);
                  out.collect(name);
              }
          }
        })
    .print();

// execute unified pipeline
env.execute();

// prints (in both BATCH and STREAMING mode):
// +I[Bob, 239]
// +I[Alice, 122]
// +I[Bob, 999]
//
// Bob
// Alice
```

#### 3、仅插入流处理（Handling of (Insert-Only) Streams）

`StreamTableEnvironment` 提供了如下方法来和 DataStream API 之间进行转换：

- `fromDataStream(DataStream)`：将 insert-only changes 和任意类型的流解释为表。事件时间和水印默认是不传播的（propagated）。
- `fromDataStream(DataStream, Schema)`：将 insert-only changes 和任意类型的流解释为表。可选的 `Schema` 可以扩展字段数据类型和增加时间属性、水印类型、其它用于计算的字段、或主键。
- `createTemporaryView(String, DataStream)`：将流注册为可以通过 SQL 访问的名称。它是 `createTemporaryView(String, fromDataStream(DataStream))` 方法的简便形式。
- `createTemporaryView(String, DataStream, Schema)`：将流注册为可以通过 SQL 访问的名称。它是 `createTemporaryView(String, fromDataStream(DataStream, Schema))` 方法的简便形式。
- `toDataStream(Table)`：将表转换为 insert-only changes 的流。默认的流记录类型是 `org.apache.flink.types.Row`。仅仅一个行时间（rowtime）属性字段会被写回到 DataStream API 的记录中。水印也会被传播。
- `toDataStream(Table, AbstractDataType)`：将表转换为 insert-only changes 的流。方法接收一个数据类型来表示期望的流记录类型。计划器（palnner）可能会插入隐式转换（implicit casts）和重新排列列（reorders columns）来映射字段和数据类型（possibly nested，可能有嵌套）。
- `toDataStream(Table, Class)`：`toDataStream(Table, DataTypes.of(Class))` 的简便形式，用于以反射方式（reflectively）快速创建想要的数据类型。

从 Table API 的角度来看，与 DataStream API 之间的转换类似于读或者写一个（使用 SQL 中的 DDL `CREATE TABLE` 创建的）虚拟的表连接器（virtual table connector）。

虚拟的 `CREATE TABLE name (schema) WITH (options)` 语句的 schema 部分可以自动地从 DataStream 的类型信息获取，可以手动地使用 `org.apache.flink.table.api.Schema` 进行扩展或者整个的定义。

虚拟的 DataStream 表连接器（connector）公开了每行的以下元数据：

| Key     | Data Type                 | Description    | R/W  |
| ------- | ------------------------- | -------------- | ---- |
| rowtime | TIMESTAMP_LTZ(3) NOT NULL | 流记录的时间戳 | R/W  |

虚拟的 DataStream 表源（source）实现了 `SupportsSourceWatermark` 因此可以调用 `SOURCE_WATERMARK()` 内置函数作为一个水印策略来从 DataStream API 获取水印。

##### 3.1、`fromDataStream` 的例子

不同场景中 `fromDataStream` 的使用：

```scala
import org.apache.flink.api.scala._
import java.time.Instant;

// some example case class
case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)

// create a DataStream
val dataStream = env.fromElements(
    User("Alice", 4, Instant.ofEpochMilli(1000)),
    User("Bob", 6, Instant.ofEpochMilli(1001)),
    User("Alice", 10, Instant.ofEpochMilli(1002)))


// === 例 1 ===

// 自动获取所有的物理列（physical columns）

val table = tableEnv.fromDataStream(dataStream)
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9)
// )


// === 例 2 ===

// 自动获取所有的物理列
// 但是增加运算列（computed columns） (这里是创建一个处理时间（proctime）属性列（attribute column）)

val table = tableEnv.fromDataStream(
    dataStream,
    Schema.newBuilder()
        .columnByExpression("proc_time", "PROCTIME()")
        .build())
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT NOT NULL,
//  `event_time` TIMESTAMP_LTZ(9),
//  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
//)


// === 例 3 ===

// 自动获取所有的物理列
// 但是增加运算列 (这里是创建一个 rowtime 属性列)和一个自定义的水印策略（watermark strategy）

val table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
            .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
            .build())
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9),
//  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
//  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
// )


// === 例 4 ===

// 自动获取所有的物理列
// 但是访问流记录的时间戳来创建一个 rowtime 属性列
// 也依赖于 DataStream API 中产生的水印

// we assume that a watermark strategy has been defined for `dataStream` before
// (not part of this example)
val table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
            .watermark("rowtime", "SOURCE_WATERMARK()")
            .build())
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9),
//  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
//  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
// )


// === 例 5 ===

// 手动地定义物理列
// 在这个例子中,
//   - 可以将时间错精度从 9 减少为 3
//   - 也投影了（project）列并把 `eventtime` 列放在开头

val table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .column("event_time", "TIMESTAMP_LTZ(3)")
            .column("name", "STRING")
            .column("score", "INT")
            .watermark("event_time", "SOURCE_WATERMARK()")
            .build())
table.printSchema()
// prints:
// (
//  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
//  `name` VARCHAR(200),
//  `score` INT
// )
// 注意:由于插入的列重新排序投影（inserted column reordering projection），水印策略没有显示
```

例 1 是没有基于时间的操作时的简单用法

例 4 是基于时间的操作（比如开窗（windows）或者间隔连接（interval joins））是管道的一部分时最常用的用法。

例 2 是基于时间的操作使用处理时间是最常用的用法。

例 5 整体上依赖于用户的声明。在用合适的数据类型替换来自 DataStream API的泛型（generic type（在 Table API 中是 `RAW`））方面很有用。

由于 `DataType` 比 `TypeInformation` 更丰富，可以很容易地启用不可变的 POJOs 和其它复杂的数据结构。Java 例子：

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;

// DataStream API 还不支持不可变得 POJOs,
// 默认情况下，这个类在 Table API 中会是一个 RAW 类型的泛型
public static class User {

    public final String name;

    public final Integer score;

    public User(String name, Integer score) {
        this.name = name;
        this.score = score;
    }
}

// create a DataStream
DataStream<User> dataStream = env.fromElements(
    new User("Alice", 4),
    new User("Bob", 6),
    new User("Alice", 10));

// 由于不能访问 RAW 类型的属性，每一条流记录被当作一个原子类型，导致 table 的 schema 只有一列 `f0`

Table table = tableEnv.fromDataStream(dataStream);
table.printSchema();
// prints:
// (
//  `f0` RAW('User', '...')
// )

// 作为替代，使用 Table API 的类型系统在自定义的 schema 中为列声明一个更有用的数据类型
// 并在随后的投影（projection）中使用 `as` 对列进行重命名

Table table = tableEnv
    .fromDataStream(
        dataStream,
        Schema.newBuilder()
            .column("f0", DataTypes.of(User.class))
            .build())
    .as("user");
table.printSchema();
// prints:
// (
//  `user` *User<`name` STRING,`score` INT>*
// )

// 可以像上面那样通过反射（reflectively）提取数据类型，也可以明确地（explicitly）定义

Table table3 = tableEnv
    .fromDataStream(
        dataStream,
        Schema.newBuilder()
            .column(
                "f0",
                DataTypes.STRUCTURED(
                    User.class,
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("score", DataTypes.INT())))
            .build())
    .as("user");
table.printSchema();
// prints:
// (
//  `user` *User<`name` STRING,`score` INT>*
// )
```

##### 3.2、`createTemporaryView` 的例子

`DataStream` 可以直接注册为视图（view）（可能通过 schema 进行扩展）。

从 DataStream 创建的视图只能被注册为临时视图。因为它们的内联/匿名（inline/anonymous）本质，不可能将它们注册到持久的 catalog 中。

不同场景下，`createTemporaryView` 的使用：

```scala
// create some DataStream
val dataStream: DataStream[(Long, String)] = env.fromElements(
    (12L, "Alice"),
    (0L, "Bob"))


// === 例 1 ===

// 在当前 session 中注册 DataStream 为 “MyView” 视图
// 所有的列都是自动获取的

tableEnv.createTemporaryView("MyView", dataStream)

tableEnv.from("MyView").printSchema()

// prints:
// (
//  `_1` BIGINT NOT NULL,
//  `_2` STRING
// )


// === 例 2 ===

// 在当前 session 中注册 DataStream 为 “MyView” 视图
// 与 `fromDataStream` 类似，提供 schema 来匹配字段

// 在本例中，提取的 NOT NULL 信息被移除了

tableEnv.createTemporaryView(
    "MyView",
    dataStream,
    Schema.newBuilder()
        .column("_1", "BIGINT")
        .column("_2", "STRING")
        .build())

tableEnv.from("MyView").printSchema()

// prints:
// (
//  `_1` BIGINT,
//  `_2` STRING
// )


// === 例 3 ===

// 如果创建视图只是重命名了列，可以在创建视图之前使用 Table API

tableEnv.createTemporaryView(
    "MyView",
    tableEnv.fromDataStream(dataStream).as("id", "name"))

tableEnv.from("MyView").printSchema()

// prints:
// (
//  `id` BIGINT NOT NULL,
//  `name` STRING
// )
```

##### 3.3、`toDataStream` 的例子

不同场景下，`toDataStream` 的使用：

```scala
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.DataTypes

case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)

tableEnv.executeSql(
  """
  CREATE TABLE GeneratedTable (
    name STRING,
    score INT,
    event_time TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
  )
  WITH ('connector'='datagen')
  """
)

val table = tableEnv.from("GeneratedTable")


// === 例 1 ===

// 使用默认的转换为 Row 的实例

// 由于 `event_time` 只是一个 rowtime 属性，它被插入到 DataStream 元数据并且水印会被传播

val dataStream: DataStream[Row] = tableEnv.toDataStream(table)


// === 例 2 ===

// 从类 `User` 提取数据类型（data type）
// planner 重新排列属性并在可能的地方插入隐式转换以将内部的数据结构转换为期望的结构化类型

// 由于 `event_time` 只是一个 rowtime 属性，它被插入到 DataStream 元数据并且水印会被传播

val dataStream: DataStream[User] = tableEnv.toDataStream(table, classOf[User])

// 可以像上面那样通过反射（reflectively）提取数据类型，也可以明确地（explicitly）定义

val dataStream: DataStream[User] =
    tableEnv.toDataStream(
        table,
        DataTypes.STRUCTURED(
            classOf[User],
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("score", DataTypes.INT()),
            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))))
```

注意，只有没有更新的（non-updating）表支持 `toDataStream`。通常，基于时间的操作（比如开窗、间隔连接、或者 `MATCH_RECOGNIZE` 子句）非常适合 insert-only 管道，仅次于（next to）touying（projection）和过滤（filters）这样的简单操作。

使用产生更新操作的管道可以使用 `toChangelogStream`。

#### 4、处理 Changelog 流

在内部，Flink 的表运行时（table runtime）是一个 changelog 处理器。

`StreamTableEnvironment` 提供了如下的方法来暴露这些变化数据获取（CDC，change data capture）功能：

- `fromChangelogStream(DataStream)`：将 changelog 条目的流（stream）转换为表（table）。流记录类型必须是 `org.apache.flink.types.Row`，因为它的 `RowKind` 标识是在运行期间获取的（evaluated）。默认事件时间和水印是不传播的。作为默认的 `ChangelogMode`，这个方法需要 changelog 包含了所有种类的变化（在 `org.apache.flink.types.RowKind` 中枚举的）。
- `fromChangelogStream(DataStream, Schema)`：与 `fromDataStream(DataStream, Schema)` 类似，能够定义一个对应 `DataStream` 的 schema。其他方面，语义与 `fromChangelogStream(DataStream)` 相同。
- `fromChangelogStream(DataStream, Schema, ChangelogMode)`：可以完全控制如何将流转换为一个 changelog。传递的 `ChangelogMode` 参数可以帮助 planner 区分行为类型是 `insert-only`、`upsert` 还是 `retract`。
- `toChangelogStream(Table)`：`fromChangelogStream(DataStream)` 的逆操作。它产生一个 `org.apache.flink.types.Row` 实例的流，并在运行时为每一条记录设置 `RowKind` 标识。这个方法支持所有类型的更新表（updating tables）。如果输入表只包含一个 rowtime 列，它会被传播到流记录的时间戳。水印也会被传播。
- `toChangelogStrean(Table, Schema)`：`fromChangelogStream(DataStream, Schema)` 的逆操作。这个方法可以扩展产生列的数据类型。如果必要 planner 可能会插入隐式转换。它可能会将 rowtime 写为一个元数据列。
- `toChangelogStream(Table, Schema, ChangelogMode)`：可以完全控制如何将一个表转换为 changlog 流。传递的 `ChangelogMode` 参数可以帮助 planner 区分行为类型是 `insert-only`、`upsert` 还是 `retract`。

从 Table API 的角度来看，与 DataStream API 的相互转化类似于读或者写一个（使用 SQL 的 `CREATE TABLE` DDL 定义的）虚拟的表连接器（connector）。

`fromChangelogStream` 行为与 `fromDataStream` 类似。

虚拟连接器也支持读和写流记录的 `rowtime` 元数据。

虚拟的表源实现了 `SupportsSourceWatermark`。

##### 4.1、`fromChangelogStream` 的例子

```scala
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.Schema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.{Row, RowKind}

// === EXAMPLE 1 ===

// interpret the stream as a retract stream

// create a changelog DataStream
val dataStream = env.fromElements(
    Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
    Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
    Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", Int.box(12)),
    Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
)(Types.ROW(Types.STRING, Types.INT))


// DataStream 转换为 Table
val table = tableEnv.fromChangelogStream(dataStream)

// register the table under a name and perform an aggregation
tableEnv.createTemporaryView("InputTable", table)
tableEnv
    .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
    .print()

// prints:
// +----+--------------------------------+-------------+
// | op |                           name |       score |
// +----+--------------------------------+-------------+
// | +I |                            Bob |           5 |
// | +I |                          Alice |          12 |
// | -D |                          Alice |          12 |
// | +I |                          Alice |         100 |
// +----+--------------------------------+-------------+


// === EXAMPLE 2 ===

// interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

// create a changelog DataStream
val dataStream = env.fromElements(
    Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
    Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
    Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
)(Types.ROW(Types.STRING, Types.INT))

// interpret the DataStream as a Table
val table =
    tableEnv.fromChangelogStream(
        dataStream,
        Schema.newBuilder().primaryKey("f0").build(),
        ChangelogMode.upsert())

// register the table under a name and perform an aggregation
tableEnv.createTemporaryView("InputTable", table)
tableEnv
    .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
    .print()

// prints:
// +----+--------------------------------+-------------+
// | op |                           name |       score |
// +----+--------------------------------+-------------+
// | +I |                            Bob |           5 |
// | +I |                          Alice |          12 |
// | -U |                          Alice |          12 |
// | +U |                          Alice |         100 |
// +----+--------------------------------+-------------+
```

##### 4.2、`toChangelogStream` 的例子

```scala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.time.Instant

// create Table with event-time
tableEnv.executeSql(
  """
  CREATE TABLE GeneratedTable (
    name STRING,
    score INT,
    event_time TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
  )
  WITH ('connector'='datagen')
  """
)

val table = tableEnv.from("GeneratedTable")


// === EXAMPLE 1 ===

// convert to DataStream in the simplest and most general way possible (no event-time)

val simpleTable = tableEnv
    .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
    .as("name", "score")
    .groupBy($"name")
    .select($"name", $"score".sum())

tableEnv
    .toChangelogStream(simpleTable)
    .executeAndCollect()
    .foreach(println)

// prints:
// +I[Bob, 12]
// +I[Alice, 12]
// -U[Alice, 12]
// +U[Alice, 14]


// === EXAMPLE 2 ===

// convert to DataStream in the simplest and most general way possible (with event-time)

val dataStream: DataStream[Row] = tableEnv.toChangelogStream(table)

// since `event_time` is a single time attribute in the schema, it is set as the
// stream record's timestamp by default; however, at the same time, it remains part of the Row

dataStream.process(new ProcessFunction[Row, Unit] {
    override def processElement(
        row: Row,
        ctx: ProcessFunction[Row, Unit]#Context,
        out: Collector[Unit]): Unit = {

        // prints: [name, score, event_time]
        println(row.getFieldNames(true))

        // timestamp exists twice
        assert(ctx.timestamp() == row.getFieldAs[Instant]("event_time").toEpochMilli)
    }
})
env.execute()


// === EXAMPLE 3 ===

// convert to DataStream but write out the time attribute as a metadata column which means
// it is not part of the physical schema anymore

val dataStream: DataStream[Row] = tableEnv.toChangelogStream(
    table,
    Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())

// the stream record's timestamp is defined by the metadata; it is not part of the Row

dataStream.process(new ProcessFunction[Row, Unit] {
    override def processElement(
        row: Row,
        ctx: ProcessFunction[Row, Unit]#Context,
        out: Collector[Unit]): Unit = {

        // prints: [name, score]
        println(row.getFieldNames(true))

        // timestamp exists once
        println(ctx.timestamp())
    }
})
env.execute()


// === EXAMPLE 4 ===

// for advanced users, it is also possible to use more internal data structures for better
// efficiency

// note that this is only mentioned here for completeness because using internal data structures
// adds complexity and additional type handling

// however, converting a TIMESTAMP_LTZ column to `Long` or STRING to `byte[]` might be convenient,
// also structured types can be represented as `Row` if needed

val dataStream: DataStream[Row] = tableEnv.toChangelogStream(
    table,
    Schema.newBuilder()
        .column(
            "name",
            DataTypes.STRING().bridgedTo(classOf[StringData]))
        .column(
            "score",
            DataTypes.INT())
        .column(
            "event_time",
            DataTypes.TIMESTAMP_LTZ(3).bridgedTo(class[Long]))
        .build())

// leads to a stream of Row(name: StringData, score: Integer, event_time: Long)
```

`toChangelogStream(Table).executeAndCollect()` 的行为与调用 `Table.execute().collect()` 相同。但是，测试时 `toChangelogStream(Table)` 可能更有用，因为在后续的 DataStream API 中 `ProcessFunction` 可以访问所生成的水印。

#### 5、将 Table API 管道加到 DataStream API

一个 Flink 作业可以由多个彼此相邻运行的断开连接的（disconnected）管道组成。

用 Table API 定义的源到汇（source-to-sink）管道可以作为一个整体（whole）附加到（attached）`StreamExecutionEnvironment`，当调用 DataStream API 的某一个 `execute` 方法时会被提交。

但是，源不必是表数据源，也可以是转换为 Table API 的 DataStream 管道数据源。这样，可以将 table 汇用于 DataStream API 程序。

通过用 `StreamTableEnvironment.createStatement()` 创建的特殊实例 `StreamStatementSet` 能够实现这个功能。通过使用 statement set，planner 可以一起优化所有被加入的 statements，并且当调用 `StreamStatementSet.attachAsDataStream()` 时，将产生一个或者多个端到端管道附加到 `StreamExecutionEnvironment`。

在同一个 job 中将 table 程序加到 DataStream API 程序的例子：

```scala
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val statementSet = tableEnv.createStatementSet()

// create some source
val sourceDescriptor = TableDescriptor.forConnector("datagen")
    .option("number-of-rows", "3")
    .schema(Schema.newBuilder
        .column("myCol", DataTypes.INT)
        .column("myOtherCol", DataTypes.BOOLEAN).build)
    .build

// create some sink
val sinkDescriptor = TableDescriptor.forConnector("print").build

// add a pure Table API pipeline
val tableFromSource = tableEnv.from(sourceDescriptor)
statementSet.addInsert(sinkDescriptor, tableFromSource)

// use table sinks for the DataStream API pipeline
val dataStream = env.fromElements(1, 2, 3)
val tableFromStream = tableEnv.fromDataStream(dataStream)
statementSet.addInsert(sinkDescriptor, tableFromStream)

// attach both pipelines to StreamExecutionEnvironment
// (the statement set will be cleared calling this method)
statementSet.attachAsDataStream()

// define other DataStream API parts
env.fromElements(4, 5, 6).addSink(new DiscardingSink[Int]())

// now use DataStream API to submit the pipelines
env.execute()

// prints similar to:
// +I[1618440447, false]
// +I[1259693645, true]
// +I[158588930, false]
// +I[1]
// +I[2]
// +I[3]
```

#### 6、Scala 中的隐式转换

通过使用 Scala 的 *implicit* 特性，Scala API 用户可以以一种更流畅的方式使用以上提及的转换方法。通过导入 `org.apache.flink.table.api.bridge.scala._` 包对象（package object），就可以使用这些隐式转换。

启用之后，像 `toTable` 或者 `toChangelogTable` 这些方法就可以直接在 `DataStream` 对象上调用。类似地，可以在 `Table` 对象上直接调用 `toDataStream` 和 `toChangelogStream` 方法。另外，当对 `DataStream[Row]` 请求一个 DataStream API 专有方法时，`Table` 对象会被转换为一个 changelog stream。

>隐式转换应该总是有意识的决定（conscious decision）。应该注意 IDE 是否通过 implicts 提议使用（proposes）一个事实上的 Table API 方法或者 DataStream API 方法。
>
>比如，`table.execute().collect()` 是 Table API 方法，而 `table.executeAndCollect()` 则隐式地使用了 DataStream API 的 `executeAndCollect()` 方法，因而强制进行了 API 转换。

```scala
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val dataStream: DataStream[(Int, String)] = env.fromElements((42, "hello"))

// call toChangelogTable() implicitly on the DataStream object
val table: Table = dataStream.toChangelogTable(tableEnv)

// force implicit conversion
val dataStreamAgain1: DataStream[Row] = table

// call toChangelogStream() implicitly on the Table object
val dataStreamAgain2: DataStream[Row] = table.toChangelogStream
```

#### 7、`TypeInformation` 和 `DataType` 之间的映射

DataStream API 使用 `org.apache.flink.api.common.typeinfo.TypeInformation` 的实例来描述流中流动的记录类型。尤其是，它定义了记录在 DataStream 算子之间如何序列化和反序列化。它也在 state 到保存点和检查点的序列化中提供帮助。

Table API 在内部使用自定义数据结构（data structures）来表示记录，并且为了在源、汇、UDF、或者  DataStream API 中更简单的应用，为用户提供了 `org.apache.flink.table.types.DataType` 来声明外部格式转换为哪种数据结构。

`DataType` 比 `TypeInformation` 更丰富（richer），它也包括了逻辑 SQL 类型（logical SQL type）的细节。因而，在转换过程中会隐式地添加某些某些细节。

列名和 `Table` 类型是自动地从 DataStream 的 `TypeInformation` 获取的。使用 `DataStream.getType()` 通过 DataStream API 的反射类型提取功能，可以检验类型信息是否被正确地侦测到。如果最外层（outermost）记录的 `TypeInformation` 是一个 `CompositeType`，在获取表 Schema 时它会被扁平化为第一级（first level）。

>DataStream API 不总是能提取更具体的（specific） `TypeInformation` （基于反射）。这经常是默默地发生的，并且是基于通用的 Kryo 序列化器产生 `GenericTypeInfo`。
>
>比如，`Row` 类不同通过反射解析，总是需要明确的类型信息声明。如果 DataStream API 中没有合适的类型信息声明，对应的行就会显示为 `RAW` 数据类型并且 Table API 将不能访问它的属性。使用 Java 的 `.map(...).returns(TypeInformation)` 或者 Scala 的 `.map(...)(TypeInformation)` 来明确地声明类型信息。

##### 7.1、TypeInfomation 转换为 DataType

`TypeInformation` 转换为 `DataType` 遵循以下规则：

- `TypeInformation` 的所有子类都映射为逻辑类型（logical types），包括与 Flink 的内置序列化器对齐的是否为 null（nuallability）。
- `TupleTypeInfoBase` 的子类被转换为一行 row（对于 `Row`）或者结构化的类型（对于元组、POJOs、和case classes）。
- `BigDecimal` 默认转换为 `DECIMAL(38,18)`。
- `PojoTypeInfo` 属性的顺序是通过一个将所有属性作为参数的构造器（constructor）决定的。如果在转换中没有找到构造器，属性顺序会是字母表顺序。
- `GenericTypeInfo` 和其它 `TypeInformation` 不能由 `org.apache.flink.table.api.DataTypes` 中列出的类型表示的会被当作黑盒（black-box）`RAW` 类型。当前 session 配置是用于实现原始类型（raw type）的序列化器的。组合嵌套属性不能被访问。
- `TypeInfoDataTypeConverter` 包含了完整的转换逻辑。

在自定义 schema 声明或者 UDFS 中，使用 `DataTypes.of(TypeInformation)` 来调用上面的逻辑。

##### 7.2、DataType 转换为 TypeInformation

表运行时会确保恰当地将输出记录序列化到 DataStream API 的第一个算子。

>之后，需要考虑 DataStream API 的类型信息语义。

#### 8、旧版转换（Legacy Conversion）

>以下章节讲的是 API 中过期的部分，未来版本可能会移除。
>
>尤其是，这些部分可能不能很好地集成到许多最近的新特性和重构

暂时省略……
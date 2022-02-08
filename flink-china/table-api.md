### Table API

#### 1、Table API 和 SQL

Table API 和 SQL 是 Flink API 中最为便捷的 API，其特点如下：

![1644215475493](/assets/1644215475493.png)

Table API 和 SQL 是声明式的 API，不用关心底层是如何实现的。

Table API 和 SQL 底层有优化器对查询进行优化，从而能获得**高性能**。假如，WordCount 的例子有两个 `count` 操作，优化器会识别并避免重复的计算，计算时只保留一个 `count` 操作，输出时将相同值输出两遍以达到更好的性能。

##### 1.1、Table API 特点

![1644215808837](/assets/1644215808837.png)

1. ##### Table API 使得多声明的数据处理起来比较容易

   假如有一个 Table（tab），需要执行过滤操作然后输出到结果表，对应的实现为：`tab.where("a < 10").insert("resultTable1")`；另外还要执行别的过滤并输出：`tab.where("a > 100").insertInto("resultTable2")`。可见，用 Table API 非常的简洁。

2. ##### Table API 是 Flink 自身的一套 API，可以更容易地去扩展标准的 SQL。

   扩展 SQL 也不是随意的，需要考虑 API 的语义、原子性和正交性，并且仅当需要时才去扩展。

   对比 SQL，可以认为 Table API 是 SQL 的超集。SQL 有的操作，Table API 可以有，另外还可以从易用性和功能性角度对 SQL 进行扩展和提升。

#### 2、Table API 编程

##### 2.1、WordCount 示例

Java 语言 Batch 版 WordCount 示例：

```java
public class JavaBatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        String path =
            JavaBatchWordCount.class.getClassLoader().getResource("words.txt").getPath();

        tEnv.connect(new FileSystem().path(path))
            .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter("\n"))
            .withSchema(new Schema().field("word", Types.STRING))
            .registerTableSource("fileSource");

        Table result = tEnv.scan("fileSource")
            .groupBy("word")
            .select("word, count(1) as count");

        tEnv.toDataSet(result, Row.class).print();
    }
}
```

首先，初始化 environment 获取 `env`，然后获得 Table 环境 `tEnv`，之后就可以注册 TableSource、TableSink 或执行一些其他操作。本例通过文件注册 TableSource，指定了文件的格式、分隔符、schema。使用 `scan` 注册的 TableSource，就可以得到 `Table` 对象并对其进行一些操作（groupBy、count），最后，将 `Table` 以 DataSet 的方式输出。

本例中，`ExecutionEnvironment` 和 `BatchTableEnvironment` 都是 Java 版本的，如果编写 Scala 程序，需要对应的 Scala 版本 API。如下为 Flink environment 相关 API 的归纳：

![1644216987462](/assets/1644216987462.png)

##### 2.2、获取 `Table`

获取 `Table` 分两步：注册对应的 `TableSource`，调用 Table environment 的 `scan` 方法获取 Table 对象。

注册 TableSource 的三种方法：通过 Table descriptor 注册；通过自定义 source 注册；通过 DataStream 注册。

![1644218404025](/assets/1644218404025.png)

##### 2.3、输出 `Table`

三种方法：通过 Table descriptor，自定义 Table Sink，输出一个 DataStream。

![1644218670033](/assets/1644218670033.png)

##### 2.4、操作 `Table`

##### 2.4.1、Table 操作总览

![1644218788390](/assets/1644218788390.png)

如图，对 `Table` 进行 `groupBy` 操作后，会返回一个 `GroupedTable`，`GroupedTable` 只有一个 `select` 操作可用，对 `GroupedTable` 执行 `select` 操作会返回一个 `Table`——然后，就可以对其执行 `Table` 的操作了。`OverWindowedTable` 用法上与 `GroupedTable` 类似。引入各类型的 Table 是为了保证 API 的合法性和便利性。

##### Table API 操作分类：

![1644219194521](/assets/1644219194521.png)

- 跟 SQL 对齐的操作，比如 select、filter、join 等
- 提升 Table API 易用性的操作
- 增强 Table API 功能性的操作

##### 2.4.2、提升 Table API 易用性的操作

假如，有一张很大的表，100 列，只有一列不用，SQL 如何写……如果 select 99 列，会很难受！为了解决这个问题，Table 上引入了 `dropColumns` 方法，在这个应用场景中只用去掉一列就可以了。此外还有 `addColumns`、`addOrReplaceColumns`、`renameColumns` 方法。

![1644219677256](/assets/1644219677256.png)

假如，100 列的表，只需要 20 ~ 80 列，该如何操作……可以使用 `withColumns` 和 `withoutColumns` 方法——`table.select("withColumns(20 to 80)")`。

![1644219992841](/assets/1644219992841.png)

##### withColumns \ withoutColumns 语法：

![1644221332694](/assets/1644221332694.png)

##### 2.4.3、增强 Table API 功能性的操作

Flink 有三种自定义函数：`ScalarFunction`、`TableFunction`、`AggregateFunction`。下图以输入、输出的维度对这些自定义函数进行了分类：

![1644221781139](/assets/1644221781139.png)

为了让语义更加完整，Table API 增加了 `TableAggregateFunction`，可以接受和输出多行。`TableAggregateFunction` 添加后，Table API 的功能得到很到的扩展，某种程度上可以用它来实现自定义的 operator，比如用 `TableAggregateFunction` 实现 TopN。

##### Map operation——易用性

![1644224383555](/assets/1644224383555.png)

##### FlatMap operation——易用性

![1644224428152](/assets/1644224428152.png)

##### Aggregate operation——易用性

![1644224939566](/assets/1644224939566.png)

##### FlatAggregate operation——功能性

![1644224695903](/assets/1644224695903.png)

##### Aggregate VS TableAggregate

使用 Max 和 Top2 应用场景比较 Aggreage 和 FlatAggregate 之间的差异

![1644224820762](/assets/1644224820762.png)
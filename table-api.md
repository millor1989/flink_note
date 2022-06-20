### Table API & SQL

Apache Flink 有两种关系型 APIs——Table API 和 SQL——用于统一的流和批处理。Table API 是一个用于 Java、Scala、Python 的语言集成（language-integrated）查询 API，支持用选择（selection）、过滤（filter）、连接（join）算子以直观的方式来构建查询。Flink 的 SQL 支持是基于实现了 SQL 标准的 Apache Calcite 的。无论输入是连续的（流式传输）还是有界的（批处理），任一接口中指定的查询都具有相同的语义并指定相同的结果。

Table API 和 SQL 接口可以相互无缝集成，也可以和 Flink 的 DataStream API 无缝集成。

#### 1、Table 程序依赖

Scala 依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge_2.11</artifactId>
  <version>1.14.4</version>
  <scope>provided</scope>
</dependency>
```

另外，如果想要在 IDE 中本地运行 Table API & SQL，那么需要加入以下模块：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-planner_2.11</artifactId>
  <version>1.14.4</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala_2.11</artifactId>
  <version>1.14.4</version>
  <scope>provided</scope>
</dependency>
```

#### 2、扩展依赖

如果想要实现一个自定义的格式或者 connector 来进行（反）序列化行，或者实现自定义函数，需要如下依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-common</artifactId>
  <version>1.14.4</version>
  <scope>provided</scope>
</dependency>
```


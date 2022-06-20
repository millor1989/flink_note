### 数据类型

Flink SQL 有丰富的原生数据类型可以供用户使用。

#### 1、数据类型

数据类型描述了表生态系统中一个值得逻辑类型。可以用来声明操作的输入、输出类型。

Flink 的数据类型与 SQL 标准数据类型术语类似，但是为了高效的处理标量表达式（scalar expressions），它还包含了是否为 NULL 的信息。

##### 1.1、Table API 中的数据类型

基于 JVM 的 API 的用户，在 Table API 中或者定义 connectors，catalogs 或 用户自定义函数时，使用 `org.apache.flink.table.types.DataType` 中的实例。

`DataType` 实例有两个责任：

- **逻辑类型声明**：并不意味着传输或存储的具体物理表示，但是定义了基于 JVM/Python 语言和表生态系统的边界
- *可选的* ：向 planner 提供有关数据物理表示的提示（hints），与其他 API 交互时很有用

对于基于 JVM 的语言，所有预定义的数据类型都在 `org.apache.flink.table.api.DataTypes` 中。

推荐在表程序中使用导入所有数据类型：

```scala
import org.apache.flink.table.api.DataTypes._

val t: DataType = INTERVAL(DAY(), SECOND(3))
```

##### 1.1.1、物理的提示（Physical Hints）

与表生态系统交互需要物理的提示，即，在不能使用基于 SQL 的数据类型系统时，需要编程专用的数据类型。提示（hints）指的是一个实现所期望的数据格式。

比如，对于逻辑的 `TIMESTAMPS`类型，数据源可以使用 `java.sql.Timestamp` 类而不是使用默认的 `java.time.LocalDateTime` 。有了这个信息，运行时能够将产生的类转换为它的内部的数据格式。反过来，数据汇可以从运行时声明它消费的数据格式。

声明一个桥接转换类（bridging conversion class）的例子：

```scala
// tell the runtime to not produce or consume java.time.LocalDateTime instances
// but java.sql.Timestamp
val t: DataType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp])

// tell the runtime to not produce or consume boxed integer arrays
// but primitive int arrays
val t: DataType = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(classOf[Array[Int]])
```

**注意**，通常只有在 API 被继承（extended）时，才需要物理的提示。预定义的源/汇/函数的用户不需要定义这些提示。表程序中的提示（比如，`field.cast(TIMESTAMP(3).bridgeTo(Timestamp.clss))`）会被忽略。

#### 2、数据类型列表

对于基于 JVM 的 Table API，数据类型在 `org.apache.flink.table.api.DataTypes` 中。

默认 planner 支持如下的 SQL 类型：

| 数据类型      | 备注                                         |
| ------------- | -------------------------------------------- |
| CHAR          |                                              |
| VARCHAR       |                                              |
| STRING        |                                              |
| BOOLEAN       |                                              |
| BINARY        |                                              |
| VARBINARY     |                                              |
| BYTES         |                                              |
| DECIMAL       | 支持固定的精度（precision）和小数位（scale） |
| TINYINT       |                                              |
| SMALLINT      |                                              |
| INTEGER       |                                              |
| BIGINT        |                                              |
| FLOAT         |                                              |
| DOUBLE        |                                              |
| DATE          |                                              |
| TIME          | 只支持 0 的精度                              |
| TIMESTAMP     |                                              |
| TIMESTAMP_LTZ |                                              |
| INTERVAL      | 只支持 MONTH 和 SECONDS(3) 的间隔            |
| ARRAY         |                                              |
| MULTISET      |                                              |
| MAP           |                                              |
| ROW           |                                              |
| RAW           |                                              |
| 结构化的类型  | 目前仅在用户自定义函数中可以使用             |

##### 2.1、字符串（Character Strings）

###### CHAR

固定长度的字符串数据类型。

声明：

```scala
DataTypes.CHAR(n)
```

*n* 为 code points 的数量。*n* 的值必须介于闭区间 `1` ~ `2,147,483,647` 。如果不指定长度，`n` 的值则是 1。

桥接至 JVM 类型：

| Java Type                                | Input | Output | Remarks                  |
| :--------------------------------------- | :---: | :----: | :----------------------- |
| `java.lang.String`                       |   X   |   X    | *Default*                |
| `byte[]`                                 |   X   |   X    | Assumes UTF-8 encoding.  |
| `org.apache.flink.table.data.StringData` |   X   |   X    | Internal data structure. |

###### VARCHAR / STRING

可变长度字符串数据类型。

声明：

```scala
DataTypes.VARCHAR(n)

DataTypes.STRING()
```

用 `VARCHAR(n)` 声明时，`n` 为 code points 的最大数量。`n` 的值必须介于闭区间 `1` ~ `2,147,483,647` 。如果不指定长度，`n` 的值则是 1。

`STRING` 是 `VARCAHR(2147483647)` 的同义词。

桥接至 JVM 类型：

| Java Type                                | Input | Output | Remarks                  |
| :--------------------------------------- | :---: | :----: | :----------------------- |
| `java.lang.String`                       |   X   |   X    | *Default*                |
| `byte[]`                                 |   X   |   X    | Assumes UTF-8 encoding.  |
| `org.apache.flink.table.data.StringData` |   X   |   X    | Internal data structure. |

##### 2.2、二进制字符串

###### BINARY

固定长度的二进制字符串（一个 bytes 序列）。

声明：

```java
DataTypes.BINARY(n)
```

`n` 为字节的数量。`n` 的值必须介于闭区间 `1` ~ `2,147,483,647` 。如果不指定长度，`n` 的值则是 1。

桥接至 JVM 类型：

| Java Type | Input | Output | Remarks   |
| :-------- | :---: | :----: | :-------- |
| `byte[]`  |   X   |   X    | *Default* |

###### VARBINARY / BYTES

可变长度的二进制字符串（一个 bytes 序列）。

声明：

```java
DataTypes.VARBINARY(n)

DataTypes.BYTES()
```

使用 `VARBINARY(n)` 声明时，`n` 为字节的数量。`n` 的值必须介于闭区间 `1` ~ `2,147,483,647` 。如果不指定长度，`n` 的值则是 1。

`BYTES` 是 `VARBINARY(2147483647)` 的同义词。

桥接至 JVM 类型：

| Java Type | Input | Output | Remarks   |
| :-------- | :---: | :----: | :-------- |
| `byte[]`  |   X   |   X    | *Default* |

##### 2.3、精确数字（Exact Numberics）

###### DECIMAL

有固定精度和小数位的十进制数数据类型。

声明：

```java
DataTypes.DECIMAL(p, s)
```

`p` 为表示所表示数的数字的数量。`s` 为所表示数的小数点右侧的数字的数量。`p` 必须在闭区间 `1` ~ `38` 之间。`s` 必须在闭区间 `0` ~ `p` 之间。`p` 的默认值是 `10`，`s` 的默认值是 `0`。

`NUMBERIC(p, s)` 和 `DEC(p, s)` 是该类型的同义词。

桥接至 JVM 类型：

| Java Type                                 | Input | Output | Remarks                  |
| :---------------------------------------- | :---: | :----: | :----------------------- |
| `java.math.BigDecimal`                    |   X   |   X    | *Default*                |
| `org.apache.flink.table.data.DecimalData` |   X   |   X    | Internal data structure. |

###### TINYINT

一个字节的有符号整数的类型，值范围 -128 ~ 127。

声明：

```java
DataTypes.TINYINT()
```

桥接至 JVM 类型：

| Java Type        | Input | Output | Remarks                              |
| :--------------- | :---: | :----: | :----------------------------------- |
| `java.lang.Byte` |   X   |   X    | *Default*                            |
| `byte`           |   X   |  (X)   | Output only if type is not nullable. |

###### SMALLINT

两个字节的有符号整数的类型，值范围 `-32,768` ~ `32,767`。

声明：

```java
DataTypes.SMALLINT()
```

桥接至 JVM 类型：

| Java Type         | Input | Output | Remarks                              |
| :---------------- | :---: | :----: | :----------------------------------- |
| `java.lang.Short` |   X   |   X    | *Default*                            |
| `short`           |   X   |  (X)   | Output only if type is not nullable. |

###### INT

4 个字节的有符号整数的类型，值范围 `-2,147,483,648 ` ~ `2,147,483,647`。

声明：

```java
DataTypes.INT()
```

`INTEGER` 是该类型的同义词。

桥接至 JVM 类型：

| Java Type           | Input | Output | Remarks                              |
| :------------------ | :---: | :----: | :----------------------------------- |
| `java.lang.Integer` |   X   |   X    | *Default*                            |
| `int`               |   X   |  (X)   | Output only if type is not nullable. |

###### BIGINT

8 个字节的有符号整数的类型，值范围 `-9,223,372,036,854,775,808  ` ~ `9,223,372,036,854,775,807`。

声明：

```java
DataTypes.BIGINT()
```

桥接至 JVM 类型：

| Java Type        | Input | Output | Remarks                              |
| :--------------- | :---: | :----: | :----------------------------------- |
| `java.lang.Long` |   X   |   X    | *Default*                            |
| `long`           |   X   |  (X)   | Output only if type is not nullable. |

##### 2.4、近似数值（Approximate Numberics）

###### FLOAT

4 字节单精度浮点数值的数据类型。

声明：

```java
DataTypes.FLOAT()
```

桥接至 JVM 类型：

| Java Type         | Input | Output | Remarks                              |
| :---------------- | :---: | :----: | :----------------------------------- |
| `java.lang.Float` |   X   |   X    | *Default*                            |
| `float`           |   X   |  (X)   | Output only if type is not nullable. |

###### DOUBLE

8 字节双精度浮点数值的数据类型。

声明：

```java
DataTypes.DOUBLE()
```

`DOUBLE PRECISION` 是该类型的同义词。

桥接至 JVM 类型：

| Java Type          | Input | Output | Remarks                              |
| :----------------- | :---: | :----: | :----------------------------------- |
| `java.lang.Double` |   X   |   X    | *Default*                            |
| `double`           |   X   |  (X)   | Output only if type is not nullable. |

##### 2.5、日期和时间

###### DATE

`年-月-日` 组成的日期的数据类型，值范围 `0000-01-01` 至 `9999-12-31`。

与 SQL 标准相比，区间的开始年份是 `0000`。

声明：

```java
DataTypes.DATE()
```

桥接至 JVM 类型：

| Java Type             | Input | Output | Remarks                                                      |
| :-------------------- | :---: | :----: | :----------------------------------------------------------- |
| `java.time.LocalDate` |   X   |   X    | *Default*                                                    |
| `java.sql.Date`       |   X   |   X    |                                                              |
| `java.lang.Integer`   |   X   |   X    | Describes the number of days since epoch.                    |
| `int`                 |   X   |  (X)   | Describes the number of days since epoch. Output only if type is not nullable. |

###### TIME

`时:分:秒[.小数秒]` 格式的，可达纳秒精度的，没有时区的数据类型。值范围 `00:00:00.000000000` ~ `23:59:59.999999999`。

与 SQL 标准相比，由于语义上与 `java.time.LocalTime` 相近，不支持闰秒（`23:59:60` 和 `23:59:61`）。不提供时区。

声明：

```java
DataTypes.TIME(p)
```

`p` 是小数秒的数字个数。`p` 值介于闭区间 `0` ~ `9`。如果不指定 `p`，`p` 默认为 0。

桥接至 JVM 类型：

| `java.time.LocalTime` | X    | X    | *Default*                                                    |
| --------------------- | ---- | ---- | ------------------------------------------------------------ |
| `java.sql.Time`       | X    | X    |                                                              |
| `java.lang.Integer`   | X    | X    | Describes the number of milliseconds of the day.             |
| `int`                 | X    | (X)  | Describes the number of milliseconds of the day. Output only if type is not nullable. |
| `java.lang.Long`      | X    | X    | Describes the number of nanoseconds of the day.              |
| `long`                | X    | (X)  | Describes the number of nanoseconds of the day. Output only if type is not nullable. |

###### TIMESTAMP

`年-月-日 时:分:秒[.小数秒]` 构成的，没有时区的，精度可达纳秒的时间戳数据类型。取值范围 `0000-01-01 00:00:00.000000000` ~ `9999-12-31 23:59:59.999999999`。

与 SQL 标准相比，由于语义上与 `java.time.LocalDateTime` 相近，不支持闰秒（`23:59:60` 和 `23:59:61`）。

不支持与 `BIGINT` （JVM `long` 类型）的转换，因为这需要时区信息。如果是与 `java.time.Instant` 类似的语义，那么使用 `TIMESTAMP_LIZ` 代替。

声明：

```java
DataTypes.TIMESTAMP(p)
```

`p` 是小数秒的数字个数。`p` 值介于闭区间 `0` ~ `9`。如果不指定 `p`，`p` 默认为 6。

`TIMESTAMP(p) WITHOUT TIME ZONE` 是该类型的同义词。

桥接至 JVM 类型：

| Java Type                                   | Input | Output | Remarks                  |
| :------------------------------------------ | :---: | :----: | :----------------------- |
| `java.time.LocalDateTime`                   |   X   |   X    | *Default*                |
| `java.sql.Timestamp`                        |   X   |   X    |                          |
| `org.apache.flink.table.data.TimestampData` |   X   |   X    | Internal data structure. |

###### TIMESTAMP WITH TIME ZONE

`年-月-日 时:分:秒[.小数秒] 时区` 构成的，精度可达纳秒的时间戳数据类型。取值范围 `0000-01-01 00:00:00.000000000 +14:59` ~ `9999-12-31 23:59:59.999999999 -14:59`。

与 `TIMESTAMP_LTZ` 相比，时区偏移信息是物理地保存在每个数据信息中的。每次运算、可视化、或者与外部系统通信时都是分别计算的。

声明：

```java
DataTypes.TIMESTAMP_WITH_TIME_ZONE(p)
```

桥接至 JVM 类型：

| Java Type                  | Input | Output | Remarks              |
| :------------------------- | :---: | :----: | :------------------- |
| `java.time.OffsetDateTime` |   X   |   X    | *Default*            |
| `java.time.ZonedDateTime`  |   X   |        | Ignores the zone ID. |

###### TIMESTAMP_LTZ

`年-月-日 时:分:秒[.小数秒] 时区` 构成的，使用本地时区的，精度可达纳秒的时间戳数据类型。取值范围 `0000-01-01 00:00:00.000000000 +14:59` ~ `9999-12-31 23:59:59.999999999 -14:59`。

这种类型通过根据配置的会话时区解释 UTC 时间戳，填补了没有时区和强制使用时区的时间戳类型间的鸿沟。

声明：

```java
DataTypes.TIMESTAMP_LTZ(p)
DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)
```

`p` 是小数秒的数字个数。`p` 值介于闭区间 `0` ~ `9`。如果不指定 `p`，`p` 默认为 6。

`TIMESTAMP(p) WITH LOCAL TIME ZONE` 是该类型的同义词。

桥接至 JVM 类型：

| Java Type                                   | Input | Output | Remarks                                                      |
| :------------------------------------------ | :---: | :----: | :----------------------------------------------------------- |
| `java.time.Instant`                         |   X   |   X    | *Default*                                                    |
| `java.lang.Integer`                         |   X   |   X    | Describes the number of seconds since epoch.                 |
| `int`                                       |   X   |  (X)   | Describes the number of seconds since epoch. Output only if type is not nullable. |
| `java.lang.Long`                            |   X   |   X    | Describes the number of milliseconds since epoch.            |
| `long`                                      |   X   |  (X)   | Describes the number of milliseconds since epoch. Output only if type is not nullable. |
| `java.sql.Timestamp`                        |   X   |   X    | Describes the number of milliseconds since epoch.            |
| `org.apache.flink.table.data.TimestampData` |   X   |   X    | Internal data structure.                                     |

###### INTERVAL YEAR TO MONTH

一组年-月间隔类型的数据类型。

声明：



桥接至 JVM 类型：

###### INTERVAL DAY TO SECOND

声明：

桥接至 JVM 类型：

##### 2.6、结构化的数据类型

###### ARRAY

有相同类型子元素的数组的数据类型。

###### MAP

映射键（包括 `NULL`）和值（包括 `NULL`）的关联数组的数据类型。map 不能包含重复键，每个键只能映射到一个值。

元素类型没有限制，用户要负责保证唯一性。

###### MULTISET

多集（包，bag）数据类型。与集合不同，每个元素可以有多个相同子类型的实例。

###### ROW

字段序列的数据类型

##### 2.7、用户自定义数据类型

声明：

```java
class User {

    // extract fields automatically
    public int age;
    public String name;

    // enrich the extraction with precision information
    public @DataTypeHint("DECIMAL(10, 2)") BigDecimal totalBalance;

    // enrich the extraction with forcing using RAW types
    public @DataTypeHint("RAW") Class<?> modelClass;
}

DataTypes.of(User.class);
```

##### 2.8、其它数据类型

###### BOOLEAN

布尔数据类型，三个可能的逻辑值 `TRUE`、`FALSE`、`UNKNOWN`。

###### RAW

任意序列化的类型的数据类型。这个类型在表生态系统中是黑盒的，只在与其它 API 交互时反序列化。

###### NULL

无类型的 `NULL` 值得数据类型。

#### 3、转换（Casting）

Flink Table API 和 SQL 可以在 `input` 类型和 `target` 类型之间执行转换。但是，不管输入值是什么，某些转换能总是成功，某些在运行时会失败（即，无法创建目标类型值时）。比如，`INT` 总能转换为 `STRING`，但是反之则不总是成功。

在制定计划阶段（planning stage），查询验证器会用 `ValidationException` 来拒绝有无效类型转换对的查询，比如，尝试将 `TIMESTAMP` 转换为 `INTERVAL` 时。查询验证器接收的有效类型转换对在运行时可能失败，这需要用户正确地处理失败。

在 Flink Table API 和 SQL 中，可以使用如下内置函数执行转换：

- `CAST`：SQL 标准定义的常规转换函数。如果转换操作是可失败的（fallible）并且提供的输入无效，会导致作业失败。类型推断会保留输入类型的可空性（nullability）。
- `TRY_CAST`：常规转换函数的扩展，转换失败时，返回 `NULL`。返回类型总是可空的（nullable）。

比如：

```sql
CAST('42' AS INT) --- returns 42 of type INT NOT NULL
CAST(NULL AS VARCHAR) --- returns NULL of type VARCHAR
CAST('non-number' AS INT) --- throws an exception and fails the job

TRY_CAST('42' AS INT) --- returns 42 of type INT
TRY_CAST(NULL AS VARCHAR) --- returns NULL of type VARCHAR
TRY_CAST('non-number' AS INT) --- returns NULL of type INT
COALESCE(TRY_CAST('non-number' AS INT), 0) --- returns 0 of type INT NOT NULL
```

支持的转换对矩阵。`Y` 表示支持， `!` 表示可失败，`N` 表示不支持：

| Input\Target                   | `CHAR`¹/ `VARCHAR`¹/ `STRING` | `BINARY`¹/ `VARBINARY`¹/ `BYTES` | `BOOLEAN` | `DECIMAL` | `TINYINT` | `SMALLINT` | `INTEGER` | `BIGINT` | `FLOAT` | `DOUBLE` | `DATE` | `TIME` | `TIMESTAMP` | `TIMESTAMP_LTZ` | `INTERVAL` | `ARRAY` | `MULTISET` | `MAP` | `ROW` | `STRUCTURED` | `RAW` |
| :----------------------------- | :---------------------------: | :------------------------------: | :-------: | :-------: | :-------: | :--------: | :-------: | :------: | :-----: | :------: | :----: | :----: | :---------: | :-------------: | :--------: | :-----: | :--------: | :---: | :---: | :----------: | :---: |
| `CHAR`/ `VARCHAR`/ `STRING`    |               Y               |                !                 |     !     |     !     |     !     |     !      |     !     |    !     |    !    |    !     |   !    |   !    |      !      |        !        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `BINARY`/ `VARBINARY`/ `BYTES` |               Y               |                Y                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `BOOLEAN`                      |               Y               |                N                 |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `DECIMAL`                      |               Y               |                N                 |     N     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TINYINT`                      |               Y               |                N                 |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `SMALLINT`                     |               Y               |                N                 |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `INTEGER`                      |               Y               |                N                 |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     Y⁵     |    N    |     N      |   N   |   N   |      N       |   N   |
| `BIGINT`                       |               Y               |                N                 |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     Y⁶     |    N    |     N      |   N   |   N   |      N       |   N   |
| `FLOAT`                        |               Y               |                N                 |     N     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `DOUBLE`                       |               Y               |                N                 |     N     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `DATE`                         |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   Y    |   N    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TIME`                         |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   Y    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TIMESTAMP`                    |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   Y    |   Y    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TIMESTAMP_LTZ`                |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   Y    |   Y    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `INTERVAL`                     |               Y               |                N                 |     N     |     N     |     N     |     N      |    Y⁵     |    Y⁶    |    N    |    N     |   N    |   N    |      N      |        N        |     Y      |    N    |     N      |   N   |   N   |      N       |   N   |
| `ARRAY`                        |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |   !³    |     N      |   N   |   N   |      N       |   N   |
| `MULTISET`                     |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     !³     |   N   |   N   |      N       |   N   |
| `MAP`                          |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |  !³   |   N   |      N       |   N   |
| `ROW`                          |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |  !³   |      N       |   N   |
| `STRUCTURED`                   |               Y               |                N                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      !³      |   N   |
| `RAW`                          |               Y               |                !                 |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |  Y⁴   |

注意：

1. 所有的固定长度或可变长度转换会根据类型定义进行 trim 或 pad。
2. 必须使用 `TO_TIMESTAMP` 和 `TO_TIMESTAMP_LTZ` 替代 `CAST / TRY_CAST`。
3. 子类型对支持时才支持。子类型对可失败时才可失败。
4. `RAW` 类和序列化器相等时支持。
5. `INTERVAL` 是 `MONT TO YEAR` 区间时支持。
6. `INTERVAL` 是 `DAY TO TIME` 区间时支持。

`NULL` 值得转换结果总是 `NULL`。

#### 4、数据类型提取（Data Type Extraction）

在 Java/Scala API 的许多地方，Flink 会使用反射从类信息中自动提取数据类型，以避免重复的人工 schema 工作。但是，因为逻辑信息可能丢失，提取数据类型相对地不总是成功的。因此，为了支持提取逻辑，提供额外的类或字段声明信息可能是必要的。

不需要额外信息就可以隐式地映射为数据类型的类的列表。

用 Scala 实现类的时候，推荐使用包装类型（boxed types）替换 Scala 的基本类型。Scala 的基本类型（比如`Int` 或 `Double`）会被编译为 JVM 基本类型（比如 `int/double`），结果是 `NOT NULL` 的语义。另外，用在泛型中（比如，`java.util.Map[Int, Double]`）中的 Scala 基本类型仔编译期间会被擦除，会导致类型信息与 `java.util.Map[java.lang.Object, java.lang.Object]` 类似。

| Class                      | Data Type                     |
| :------------------------- | :---------------------------- |
| `java.lang.String`         | `STRING`                      |
| `java.lang.Boolean`        | `BOOLEAN`                     |
| `boolean`                  | `BOOLEAN NOT NULL`            |
| `java.lang.Byte`           | `TINYINT`                     |
| `byte`                     | `TINYINT NOT NULL`            |
| `java.lang.Short`          | `SMALLINT`                    |
| `short`                    | `SMALLINT NOT NULL`           |
| `java.lang.Integer`        | `INT`                         |
| `int`                      | `INT NOT NULL`                |
| `java.lang.Long`           | `BIGINT`                      |
| `long`                     | `BIGINT NOT NULL`             |
| `java.lang.Float`          | `FLOAT`                       |
| `float`                    | `FLOAT NOT NULL`              |
| `java.lang.Double`         | `DOUBLE`                      |
| `double`                   | `DOUBLE NOT NULL`             |
| `java.sql.Date`            | `DATE`                        |
| `java.time.LocalDate`      | `DATE`                        |
| `java.sql.Time`            | `TIME(0)`                     |
| `java.time.LocalTime`      | `TIME(9)`                     |
| `java.sql.Timestamp`       | `TIMESTAMP(9)`                |
| `java.time.LocalDateTime`  | `TIMESTAMP(9)`                |
| `java.time.OffsetDateTime` | `TIMESTAMP(9) WITH TIME ZONE` |
| `java.time.Instant`        | `TIMESTAMP_LTZ(9)`            |
| `java.time.Duration`       | `INTERVAL SECOND(9)`          |
| `java.time.Period`         | `INTERVAL YEAR(4) TO MONTH`   |
| `byte[]`                   | `BYTES`                       |
| `T[]`                      | `ARRAY<T>`                    |
| `java.util.Map<K, V>`      | `MAP<K, V>`                   |
| structured type `T`        | anonymous structured type `T` |

本文档中提到的其它 JVM 桥接类则需要 `@DataTypeHint` 注解。

```scala
import org.apache.flink.table.annotation.DataTypeHint

class User {

    // defines an INT data type with a default conversion class `java.lang.Integer`
    @DataTypeHint("INT")
    var o: AnyRef

    // defines a TIMESTAMP data type of millisecond precision with an explicit conversion class
    @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class)
    var o: AnyRef

    // enrich the extraction with forcing using a RAW type
    @DataTypeHint("RAW")
    var modelClass: Class[_]

    // defines that all occurrences of java.math.BigDecimal (also in nested fields) will be
    // extracted as DECIMAL(12, 2)
    @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2)
    var stmt: AccountStatement

    // defines that whenever a type cannot be mapped to a data type, instead of throwing
    // an exception, always treat it as a RAW type
    @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
    var model: ComplexModel
}
```


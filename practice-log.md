### 1、Standalone 模式

虽然，新版本的 Flink 在 Win 7 上无法启动，但是较早版本，还是可以在 Win 7 上运行的，比如 [Flink 1.9.0](https://archive.apache.org/dist/flink/flink-1.9.0/flink-1.9.0-bin-scala_2.12.tgz)。

运行 `flink-1.9.0\bin\start-cluster.bat` 以 Standalone 模式启动 Flink。会同时启动两个 cmd 窗口，其中一个对应的是 TaskManager 的标准输出（stdout）。

![1670228155614](D:\gbp\gitbooks\flink_note\assets\1670228155614.png)

运行 `jps` 查看进程，看到一个 `TaskManager`，一个 `StandaloneSessionClusterEntrypoint`：

```
123332 TaskManagerRunner
140028 StandaloneSessionClusterEntrypoint
```

通过 `localhost:8081` 可以进入 Flink 的 WebUI。

### 2、`flink` 命令(flink 1.14)

运行`flink -h` 或者 `flink --help` 查看 `flink` 命令的帮助文档。

#### 2.1、`run` 行为

编译并运行一个程序。

语法：`run [OPTIONS] <jar-file> <arguments>`

##### `run` 行为的选项（options）

| 选项                                      | 描述                                                         |
| ----------------------------------------- | ------------------------------------------------------------ |
| `-c`，`--class` `<classname>`             | 具有程序入口（`main()` 方法）的类（Class）。当 JAR 文件的 manifest 文件中没有指定 class 的时候需要指定该选项。 |
| `-C`，`--classpath` `<url>`               | 添加一个 URL 到集群中所有节点的用户代码类加载器。路径必须指定一个协议（比如，`file://`，必须是 `java.net.URLClassLoader` 支持的协议）并且是对所有的节点来说都是能够访问的。可以使用该选项多次以指定多个 URL。 |
| `-d`，`--detached`                        | 如果提供，则以分离模式（detached mode）运行                  |
| `-n`，`--allowNonRestoredState`           | 允许跳过不能被恢复的保存点状态（savepoint state）。如果从程序中移除了一个保存点触发时存在的算子，那么需要使用这个选项。 |
| `-p`，`--parallelism` `<parallelism>`     | 用于运行程序的并行度。是一个替代配置中指定的默认值的可选标识。 |
| `-s`，`--fromSavepoint` `<savepointPath>` | 用于恢复作业的保存点的路径（比如，`hdfs:///flink/savepoint-1537`）。 |
| `-sae`，`--shutdownOnAttachedExit`        | 如果作业是以附加模式提交的，那么当客户端突然结束（比如，响应用户的中断——输入了 Ctrl + C），那么会尽力关闭集群。 |

##### 通用 CLI（Command-Line Interface）模式的选项

| 选项                        | 描述                                                         |
| --------------------------- | ------------------------------------------------------------ |
| `-D <property=value>`       | 用于指定多个通用配置选项。可用的配置见[链接](https://nightlies.apache.org/flink/flink-docs-stable/ops/config.html) |
| `-e`，`--ex1ecutor` `<arg>` | **废弃**，使用 `-t` 选项（”Appliaction Mode“ 也可以使用）替代。用于执行指定作业的 executor 名称，与配置选项 `execution.target` 等价。目前可用的 executors 是： `remote`、`local`， `kubernetes-session`， `yarn-per-job`，`yarn-session`。 |
| `-t`，`--target` `<arg>`    | 指定应用的部署目标，与配置选项 `execution.target` 等价。对于 `run` 行为来说，目前可用的目标是：`remote`， `local`， `kubernetes-session`， `yarn-per-job`，`yarn-session`。对于 `run-application` 行为来说，目前可用的目标是：`kubernetes-application`。 |

##### `yarn-cluster` 模式的选项

| 选项                                  | 描述                                              |
| ------------------------------------- | ------------------------------------------------- |
| `-m`，`--jobmanager` `<arg>`          | 设置为 yarn-cluster 以使用 YARN 执行模式。        |
| `-yid`，`--yarnapplicationId` `<arg>` | 使用运行中的 YARN session。                       |
| `-z`，`--zookeeperNamespace` `<arg>`  | 高可用模式时创建 Zookeeper sub-paths 的命名空间。 |

##### 默认模式的选项

| 选项                                 | 描述                                                         |
| ------------------------------------ | ------------------------------------------------------------ |
| `-D <property=value>`                | 同上                                                         |
| `-m`，`--jobmanager` `<arg>`         | 要连接的 JobManager 的地址。使用这个标识连接到不同于配置中指定的 JobManager。注意：这个选项只有在高可用配置为 `NONE` 时才会有效。 |
| `-z`，`--zookeeperNamespace` `<arg>` | 同上                                                         |

#### 2.2、`run-application` 行为

以 Application Mode 运行应用。

语法：`run-application [OPTIONS] <jar-file> <arguments>`

##### 通用客户端模式的选项

| 选项                       | 描述 |
| -------------------------- | ---- |
| `-D <property=value>`      | 同上 |
| `-e`，`--executor` `<arg>` | 同上 |
| `-t`，`--target` `<arg>`   | 同上 |

#### 2.3、`info` 行为

展示程序的优化后的执行计划（JSON）。

语法：`info [OPTIONS] <jar-file> <arguments>`

##### `info` 行为选项

| 选项                                  | 描述 |
| ------------------------------------- | ---- |
| `-c`，`--class` `<classname>`         | 同上 |
| `-p`，`--parallelism` `<parallelism>` | 同上 |

#### 2.4、`list` 行为

列出运行中和已调度的程序。

语法：`list  [OPTIONS]`

##### `list` 行为选项

| 选项                | 描述                              |
| ------------------- | --------------------------------- |
| `-a`，`--all`       | 展示所有的程序和他们的 JobIDs     |
| `-r`，`--running`   | 只展示运行中的程序和它们的 JobIDs |
| `-s`，`--scheduled` | 只展示已调度的程序和它们的 JobIDs |

##### 通用 CLI（Command-Line Interface）模式的选项

| 选项                        | 描述 |
| --------------------------- | ---- |
| `-D <property=value>`       | 同上 |
| `-e`，`--ex1ecutor` `<arg>` | 同上 |
| `-t`，`--target` `<arg>`    | 同上 |

##### `yarn-cluster` 模式的选项

| 选项                                  | 描述                                       |
| ------------------------------------- | ------------------------------------------ |
| `-m`，`--jobmanager` `<arg>`          | 设置为 yarn-cluster 以使用 YARN 执行模式。 |
| `-yid`，`--yarnapplicationId` `<arg>` | 同上                                       |
| `-z`，`--zookeeperNamespace` `<arg>`  | 同上                                       |

##### 默认模式的选项

| 选项                                 | 描述                                                         |
| ------------------------------------ | ------------------------------------------------------------ |
| `-D <property=value>`                | 同上                                                         |
| `-m`，`--jobmanager` `<arg>`         | 要连接的 JobManager 的地址。使用这个标识连接到不同于配置中指定的 JobManager。注意：这个选项只有在高可用配置为 `NONE` 时才会有效。 |
| `-z`，`--zookeeperNamespace` `<arg>` | 同上                                                         |

#### 2.5、`stop` 行为

`stop` 行为使用一个保存点（仅流作业）停止运行中的程序。

语法：`stop [OPTIONS] <Job ID>`

##### `stop` 行为选项

| 选项                                      | 描述                                                         |
| ----------------------------------------- | ------------------------------------------------------------ |
| `-d`，`--drain`                           | 在执行保存点操作和停止管道之前发送 MAX_WATERMARK             |
| `-p`，`--savepointPath` `<savepointPath>` | 保存点路径（比如：`hdfs:///flink/savepoint-1537`）。如果不指定则使用默认值（`state.savepoints.dir`）。 |


##### 通用 CLI（Command-Line Interface）模式的选项

| 选项                        | 描述 |
| --------------------------- | ---- |
| `-D <property=value>`       | 同上 |
| `-e`，`--ex1ecutor` `<arg>` | 同上 |
| `-t`，`--target` `<arg>`    | 同上 |

##### `yarn-cluster` 模式的选项

| 选项                                  | 描述                                       |
| ------------------------------------- | ------------------------------------------ |
| `-m`，`--jobmanager` `<arg>`          | 设置为 yarn-cluster 以使用 YARN 执行模式。 |
| `-yid`，`--yarnapplicationId` `<arg>` | 同上                                       |
| `-z`，`--zookeeperNamespace` `<arg>`  | 同上                                       |

##### 默认模式的选项

| 选项                                 | 描述                                                         |
| ------------------------------------ | ------------------------------------------------------------ |
| `-D <property=value>`                | 同上                                                         |
| `-m`，`--jobmanager` `<arg>`         | 要连接的 JobManager 的地址。使用这个标识连接到不同于配置中指定的 JobManager。注意：这个选项只有在高可用配置为 `NONE` 时才会有效。 |
| `-z`，`--zookeeperNamespace` `<arg>` | 同上                                                         |

#### 2.6、`cancel` 行为

取消运行中的程序。

语法：`cancel [OPTIONS] <Job ID>`

##### `cancel` 行为选项

| 选项                                      | 描述                                                       |
| ----------------------------------------- | ---------------------------------------------------------- |
| `-s`，`--withSavepoint <targetDirectory>` | **废弃**：取消作业并执行保存点被废弃了，使用 `stop` 代替。 |

#### 2.7、`savepoint` 行为

触发运行中作业的保存点，或者处理已存在的保存点。

语法：`savepoint [OPTIONS] <Job ID> [<target directory>]`

##### `savepoint` 行为选项

| 选项                          | 描述                |
| ----------------------------- | ------------------- |
| `-d`，`--dispose` `<arg>`     | 放置保存点的路径    |
| `-j`，`--jarfile` `<jarfile>` | Flink 程序 JAR 文件 |

##### 通用 CLI（Command-Line Interface）模式的选项

| 选项                        | 描述 |
| --------------------------- | ---- |
| `-D <property=value>`       | 同上 |
| `-e`，`--ex1ecutor` `<arg>` | 同上 |
| `-t`，`--target` `<arg>`    | 同上 |

##### `yarn-cluster` 模式的选项

| 选项                                  | 描述                                       |
| ------------------------------------- | ------------------------------------------ |
| `-m`，`--jobmanager` `<arg>`          | 设置为 yarn-cluster 以使用 YARN 执行模式。 |
| `-yid`，`--yarnapplicationId` `<arg>` | 同上                                       |
| `-z`，`--zookeeperNamespace` `<arg>`  | 同上                                       |

##### 默认模式的选项

| 选项                                 | 描述                                                         |
| ------------------------------------ | ------------------------------------------------------------ |
| `-D <property=value>`                | 同上                                                         |
| `-m`，`--jobmanager` `<arg>`         | 要连接的 JobManager 的地址。使用这个标识连接到不同于配置中指定的 JobManager。注意：这个选项只有在高可用配置为 `NONE` 时才会有效。 |
| `-z`，`--zookeeperNamespace` `<arg>` | 同上                                                         |

### 3、CDH 版本的 `flink` 命令

`flink` 的 `run` 行为，对于 `yarn-cluster` 模式会多几个选项：

| 选项                                      | 描述                                     |
| ----------------------------------------- | ---------------------------------------- |
| `-yat`，`--yarnapplicationType` `<arg>`   | 为 YARN 上的应用指定一个自定义的应用类型 |
| `-yD <property=value>`                    | 使用指定的属性-值                        |
| `-yh`， `--yarnhelp`                      | 查看 YARN session CLI 的帮助             |
| `-yid`，`--yarnapplicationId` `<arg>`     | 附加到运行中的 YARN session              |
| `-yj`， `--yarnjar` `<arg>`               | Flink jar 文件路径                       |
| `-yjm`， `--yarnjobManagerMemory` `<arg>` | JobManager 容器的内存（默认单位：MB）    |
|                                           |                                          |
|                                           |                                          |
|                                           |                                          |

### 4、`execution.target`

`flink run -t <execution.target>` 执行目标

没有找到 CDH 的默认 `execution.target` 值（看官方文档  flink on yarn 时，默认的 `execution.target` 似乎应该是 `yarn-session` 吧？，官方文档中，使用 `yarn-per-job` 时，是明确地使用 `flink run -t yarn-per-job` 指明了 `yarn-per-job` 的）。

在 Flink WebUI 的 JobManager 的配置选项卡中可以看到运行时使用的配置。

![1671093036918](/assets/1671093036918.png)

### 5、并行度

似乎就是 taskmanager 的数量。

```java
// ExecutionEnvironment
public void setParallelism(int parallelism) {
        config.setParallelism(parallelism);
    }

// StreamExecutionEnvironment
public StreamExecutionEnvironment setParallelism(int parallelism) {
        config.setParallelism(parallelism);
        return this;
    }

// TableEnvironment
TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

Configuration tblConf = tableEnv.getConfig.getConfiguration();
// 设置并行度为 5
tblConf.setString("parallelism.default", "5")
```

### 6、yarn-per-job attach 提交了一个 batch 任务

job 执行完了，但是 yarn application 还在执行，虽然释放了 Taskmanager 占用的资源，但是 jobManager 还在运行。

运行如下命令可以停止 yarn application：

```bash
echo "stop" | ./bin/yarn-session.sh -id application_xxxxx_xxxx
```

停止后，yarn application 的状态为 `SUCCESS`。

`yarn-per-job`、`yarn-session` 傻傻分不清了。

### 7、如果给的资源不够，Flink 会自动申请资源吗

启动了一个 Table Batch 任务，给了 5 的并行度（parallelism.default=5），结果疯狂的占用资源，也许是读的源表比较大。似乎并行度并不决定 taskManager 的数量啊。可能是 batch 任务读取文件的并行度受文件数量的影响。


### Flink 的开发环境搭建和应用的配置、部署及运行

#### 1、Flink 开发环境部署和配置

Flink 需要类 Unix 的环境。使用 Windows 系统时，推荐使用 Win 10 系统的 Linux 子系统（WSL， Win 7 不支持安装和运行 WSL）来编译和运行。

Java 至少是 Java 8。

Flink 必须使用 Maven 3，推荐使用 Maven 3.2.5。

推荐使用 IntelliJ IDEA IDE 作为 Flink 的 IDE 工具。不建议使用 Eclipse IDE，官方说法是，Eclipse 的 Scala IDE 和 Flink 用的 scala 不兼容。

#### 2、运行 Flink 应用

##### 2.1、基本概念

![1638760857217](/assets/1638760857217.png)

Task 是 Flink 中资源调度的最小单位，在一个 DAG 图中，不能被 chain 在一起的 operator 会被分隔到不同的 Task 中。

![1638760968229](/assets/1638760968229.png)

Flink 运行时包括两类进程：

- JobManager（又称 JobMaster）：协调 Task 的分布式执行，包括调度 Task、协调创建 checkpoint 以及当 job failover 时协调各个 Task 从 checkpoint 恢复。
- TaskManager（又称 Worker）：执行 Dataflow 中的 Tasks，包括内存 buffer 的分配、DataStream 的传递等。

![1638761166824](/assets/1638761166824.png)

##### 2.2、单机 standalone 模式运行

Flink binary 目录下，执行：

```sh
./bin/start-cluster.sh
```

输出以下结果，表示启动正常：

![1639014955014](/assets/1639014955014.png)

通过 http://127.0.0.1:8081/ 可以看到 Flink 的 Web 页面。

提交一个 Word Count 的任务，可以在 web 界面看到 job。

```shell
./bin/flink run examples/streaming/WordCount.jar
```

使用 `--input` 参数可以指定本地文件作为输入：

```shell
./bin/flink run examples/streaming/WordCount.jar --input ${your_source_file}
```

###### 2.2.1、配置

本机上执行 `jps` 命令，可以看到 Flink 相关进程，主要有两个：JobManager 和 TaskManager。

Flink binary 目录下的 conf 子目录中的 flink-conf.yaml 文件是配置文件。更改配置文件需要重启 standalone 集群。

停止集群命令：

```shell
./bin/stop-cluster.sh
```

##### 2.3、多机部署 Flink standalone 集群

##### 2.4、yarn 模式 运行 Flink

![1639016198046](/assets/1639016198046.png)

相比 standalone 模式，yarn 模式运行 flink job 的优点：
● 资源按需使用，提高集群的资源利用率
● 任务有优先级，根据优先级运行作业
● 基于 YARN 调度系统，能够自动化地处理各个角色的 failover
  ○ JobManager 进程和 TaskManager 进程都由 Yarn NodeManager 监控
  ○ 如果 JobManager 进程异常退出，则 Yarn ResourceManager 会重新调度 JobManager 到其他机器
  ○ 如果 TaskManager 进程异常退出，JobManager 会收到消息并重新向 Yarn ResourceManager 申请资源，重新启动 TaskManager
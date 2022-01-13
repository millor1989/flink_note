### 本地模式安装

#### 1、下载

Flink 依赖 Java 8 或 Java 11：

```bash
java -version
```

[下载](https://flink.apache.org/zh/downloads.html)  并解压：

```bash
$ tar -xzf flink-1.13.2-bin-scala_2.11.tgz
$ cd flink-1.13.2-bin-scala_2.11
```

#### 2、启动集群

```bash
$ ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host.
Starting taskexecutor daemon on host.
```

#### 3、提交作业（Job）

Flink 的 Releases 附带了许多的示例作业。可以任意选择一个，快速部署到已运行的集群上：

```bash
$ ./bin/flink run examples/streaming/WordCount.jar
$ tail log/flink-*-taskexecutor-*.out
  (to,1)
  (be,1)
  (or,1)
  (not,1)
  (to,2)
  (be,2)
```

可以通过 Flink 的 [Web UI](http://localhost:8081/) 来监视集群的状态和正在运行的作业。

#### 4、停止集群

停止集群和所有正在运行的组件：

```bash
$ ./bin/stop-cluster.sh
```
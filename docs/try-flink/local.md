### 安装及运行

#### 1、下载

Flink 需要运行在类 Unix 的环境中，比如 Linux、Mac OS X 等，依赖 Java 8 或 Java 11：

```bash
java -version
```

[下载](https://flink.apache.org/zh/downloads.html)  并解压：

```bash
$ tar -xzf flink-1.13.2-bin-scala_2.11.tgz
$ cd flink-1.13.2-bin-scala_2.11
```

主要目录包括：

- **bin**：包含 `flink` 二进制文件和一些管理各种 jobs 和 tasks 的 bash 脚本
- **conf**：包含配置文件，比如 `flink-conf.yaml`
- **examples**：Flink 例子

#### 2、启动本地集群（local cluster）

```bash
$ ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host.
Starting taskexecutor daemon on host.
```

启动成功后，可以在后台看到 flink 进程：

```bash
$ ps aux | grep flink
```

通过 [Web UI](http://localhost:8081/) 可以看到 Flink Dashboard，并看到集群已经启动并在运行中。

#### 3、提交作业（Job）

Flink 提供了一个 CLI 工具 `bin/flink` ，可以用来运行和控制打包为 Java ARchives（JAR）的程序。提交一个 Flink job 意味着将一个 job 的 JAR 文件和相关的依赖提交到 Flink 集群，并执行。

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


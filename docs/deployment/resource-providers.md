### Standalone

单机多进程

#### 1、部署模式

##### 1.1、Application Mode

使用 `bin/standalone-job.sh` 脚本来启动一个嵌入了应用的 Flink JobManager。

应用 jar 文件需要位于 classpath 中。最简单的方式是吧 jar 文件放到 `lib/` 文件夹：

```bash
$ cp ./examples/streaming/TopSpeedWindowing.jar lib/
```

启动 JobManager：

```bash
./bin/standalone-job.sh start --job-classname org.apache.flink.streaming.examples.windowing.TopSpeedWindowing
```

然后，WebUI 可用，但是应用不会启动，因为还没有运行 TaskManagers：

```bash
$ ./bin/taskmanager.sh start
```

 **注意**：如果应用需要更多的资源，那么可以启动多个 TaskManagers。

使用脚本停止服务，如果要停止多个实例，可以调用多次，也可以使用 `stop-all`：

```bash
$ ./bin/taskmanager.sh stop
$ ./bin/standalone-job.sh stop
```

##### 1.2、Session Mode

对于Flink Standalone，启动集群然后提交任务，就是 Session Mode:

```bash
$ ./bin/start-cluster.sh

$ ./bin/flink run ./examples/streaming/TopSpeedWindowing.jar

$ ./bin/stop-cluster.sh
```

### Native Kubernetes



### YARN

Flink 服务被提交给 YARN 的资源管理器，在 YARN 节点管理器管理的机器上启动容器。Flink 的 JobManager 和 TaskManager 实例部署在这些容器中。

Flink 可以根据 JobManger 上运行的作业需要的处理 slots 的数量动态地分配和取消分配 TaskMangers 资源。

#### 1、Flink on YARN 部署模式

对于生产环境，推荐以 Application Mode 部署 Flink 应用，因为这种模式提供了更好的应用间独立性。

##### 1.1、Application Mode（应用模式）

Application Mode 会在 YARN 上启动一个 Flink 集群，应用 jar 的 `main()` 方法在 YARN 中的 JobManager 上执行。当应用完成后，这个集群会马上关闭。可以通过取消 Flink 作业或者执行 `yarn application -kill <ApplicationId>` 手动地停止集群。

```bash
./bin/flink run-application -t yarn-application ./examples/streaming/TopSpeedWindowing.jar
```

部署 Application Mode 集群后，可以对它执行取消或者进行 savepoint 等操作。

```bash
# List running job on the cluster
./bin/flink list -t yarn-application -Dyarn.application.id=application_XXXX_YY
# Cancel running job
./bin/flink cancel -t yarn-application -Dyarn.application.id=application_XXXX_YY <jobId>
```

**注意**：取消 Application Cluster 的作业是会停止集群的。

为了解锁 Application Mode 的所有潜能，可以使用 `yarn.provided.lib.dirs` 配置项，并提前上传应用 jar 到集群的所有节点都能访问的位置。示例命令：

```bash
./bin/flink run-application -t yarn-application \
	-Dyarn.provided.lib.dirs="hdfs://myhdfs/my-remote-flink-dist-dir" \
	hdfs://myhdfs/jars/my-application.jar
```

这个命令会让 job 提交变得更加轻量级，因为需要的 Flink jars 和 应用 jar 将会从指定的远程目录获取，而不是由客户端传送给集群。

##### 1.2、Session Mode（会话模式）

```bash
# we assume to be in the root directory of 
# the unzipped Flink distribution

# (0) export HADOOP_CLASSPATH
export HADOOP_CLASSPATH=`hadoop classpath`

# (1) Start YARN Session
./bin/yarn-session.sh --detached

# (2) You can now access the Flink Web Interface through the
# URL printed in the last lines of the command output, or through
# the YARN ResourceManager web UI.

# (3) Submit example job
./bin/flink run ./examples/streaming/TopSpeedWindowing.jar

# (4) Stop YARN session (replace the application id based 
# on the output of the yarn-session.sh command)
echo "stop" | ./bin/yarn-session.sh -id application_XXXXX_XXX
```

Seesion 模式有两种操作模式：

- **attached 模式**（默认）：`yarn-session.sh` 客户端提交 Flink 集群给 YARN 后，客户端保持运行状态，并追踪集群的状态。如果集群失败，客户端会展示对应得错误。如果客户端被关闭，它会通知集群关闭。
- **detached 模式**（`-d` 或者 `--detached`）：`yarn-session.sh` 客户端提交 Flink 集群给 YARN 之后，客户端返回。需要使用其它客户端调用或者 YARN 工具来停止 Flink 集群。

session 模式会在 `/tmp/.yarn-properties-<username>` 文件中创建一个隐藏的 YARN 属性，在提交作业时命令行接口会从这里获取集群信息。

提交 Flink 作业时，也可以在命令行接口手动地指定目标 YARN 集群，比如：

```bash
./bin/flink run -t yarn-session \
  -Dyarn.application.id=application_XXXX_YY \
  ./examples/streaming/TopSpeedWindowing.jar
```

可以使用如下命令重新粘连（re-attach）到 YARN session： 

```bash
./bin/yarn-session.sh -id application_XXXX_YY
```

YARN session 客户端还有一些常用设置的快捷参数（shortcut arguments），通过命令 `./bin/yarn-session.sh -h` 查看。

##### 1.3、Per-Job Mode（每个作业模式，已废弃）

>Per-job mode 只被 YARN 支持，在 Flink 1.15 已经废弃，未来会被删除。对于 YARN per-job 场景，可以转而使用 application mode。

Per-job 集群模式会在 YARN 上启动一个 Flink 集群，然后本地运行提供的应用 jar，最后提交 JobGraph 到 YARN 上的 JobManager。如果传递了 `--detached` 参数，客户端在提交完成后会停止。

作业执行完毕，YARN 集群也会停止。

```bash
./bin/flink run -t yarn-per-job --detached ./examples/streaming/TopSpeedWindowing.jar
```

部署了 Per-Job 集群之后，可以与之进行交互，执行取消运行、进行 savepoint 之类的操作：

```bash
# List running job on the cluster
./bin/flink list -t yarn-per-job -Dyarn.application.id=application_XXXX_YY
# Cancel running job
./bin/flink cancel -t yarn-per-job -Dyarn.application.id=application_XXXX_YY <jobId>
```

Per-Job 模式下，取消作业会停止集群。

### Flink on YARN 参考

#### 1、配置 Flink on YARN

[YARN 专用配置](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#yarn)

如下配置参数是 Flink on YARN 管理的，运行时它们可能会被框架覆盖：

- `jobmanager.rpc.address`：（由 Flink on YARN 动态地设置为 JobManager 容器的地址）
- `io.tmp.dirs`：（如果不设置，Flink 会根据 YARN 来设置临时目录）
- `high-availablity.cluster-id`：（自动生成的 ID，用来在 HA 服务中区分多个集群）

如果需要向 Flink 传递其它的 Hadoop 配置，可以通过 `HADOOP_CONF_DIR` 环境变量进行，这个环境变量接收一个包含 Hadoop 配置的目录。默认情况下，所有需要的 Hadoop 配置文件都是从 `HADOPP_CONF_DIR` 环境变量配置的 classpath 加载的。

#### 2、资源分配行为

如果 YARN 上的 JobManager 使用当前的资源不能运行提交的所有作业，它就会请求额外的 TaskManagers。特别是在 Session Mode，当提交了额外的作业时，如果需要 JobManager 就会分配额外的 TaskManagers。未被使用的 TaskManagers 在一个超时时间之后会被释放。

YARN 实现会接受 JobManager 和 TaskManagers 进程的内存配置。VCores 的数量默认等于每个 TaskManagers 配置的 slots 的数量。`yarn.container.vcores` 配置允许用自定义的值覆盖 vcores 的数量。为了是这个参数能够起作用，需要启用 YARN 集群的 CPU 调度。

失败的容器（包括 JobManager） 会被 YARN 替换。JobManager 容器重启的最大次数通过 `yarn.application-attempts`（默认值 1）设置。如果所有的尝试都失败，YARN 应用就会失败。

#### 3、YARN 上的高可用性
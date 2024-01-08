### 配置

所有的配置都是在 `conf/flink-conf.yaml` 中完成的，期望的格式是扁平的 YAML 键值对——`key: value`。

当启动 Flink 进程时，会解析并获取配置。改变配置文件后需要重启相关的进程才能生效。

配置默认使用默认的 Java 安装。通过设置环境变量 `JAVA_HOME` 或者更改 `conf/flink-conf.yaml` 的配置项 `env.java.home` 可以指定 Java 运行时。

通过更改 `FLINK_CONF_DIR` 环境变量可以指定配置文件目录。对于提供非会话部署的资源提供者，可以通过这种方式指定每个作业的配置。Docker 和 Standalone Kubernetes 部署不支持这种方式。对于基于 Docker 的部署，可以使用 `FLINK_PROPERTIES` 环境变量传递配置值。

对于会话集群，提供的配置仅仅用于配置执行参数，即，配置参数只影响作业，不影响底层的集群。

#### 1、基础设置

使用 Flink 默认的配置可以启动一个单节点的 Flink 会话集群。如下，是基础的分布式 Flink 需要设置的一些常见选项：

##### 1.1、主机名、端口

仅仅对于 standalone 应用或者会话部署（简单的 standalone 或者 Kubernetes）来说是必须的

如果使用的是 [Yarn](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/resource-providers/yarn/) 或者 [active Kubernetes integration](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/resource-providers/native_kubernetes/)，主机名和端口是自动发现的。

- `rest.address`，`rest.port`：这些事客户端用来连接到 Flink 的。设置为 JobManager 运行的主机名，或者 JobManger 的 REST 接口前面的服务的主机名。
- `jobmanager.rpc.address`（默认值 “localhost”）和 `jobmanager.rpc.port`（默认值 `6123`）配置是 TaskManager 用来连接到 JobManager/ResouceManager 的。设置为 JobManager 运行的主机名，或者 JobManager 的服务（Kubernetes internal）的主机名。如果设置了高可用，那么领导选择机制会自动设置这个配置，这个配置会被忽略。

##### 1.2、内存大小

默认的内存大小支持简单的流或批应用，对于获取好的性能或者复杂的应用来说太小了。

- `jobmanager.memory.process.size`：JobManager（JobMaster/ResourceManager/Dispatcher）进程的总内存大小。
- `taskmanager.memory.process.size`：TaskManager 进程的总大小。

总大小指的是进程中的所有内存使用。Flink 会分配一些内存作为 JVM 自身的内存（metaspace 等）、其余组件（JVM 堆内存、非堆内存、TaskManagers、网络、托管内存等等）自动地分配剩下的内存。

这些值配置为内存带下，比如 `1024m` 或者 `2g`

##### 1.3、并行度

- `taskmanager.numberOfTaskSlots`：每个 TaskManager 提供的槽数（默认为 1）。每个槽可以运行一个任务或者管道。为一个 TaskManager 分配多个槽可以使它们分摊所有任务或者管道的某些固定开销（比如，JVM、应用库、网络连接）。详见[任务槽和资源](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/concepts/flink-architecture/#task-slots-and-resources)。

  运行多个只包含一个槽的 TaskManagers 是一个好的起点，并且能够获得最好的任务见独立性。分配相同资源给包含多个槽的较少 TaskManagers 会增加资源的利用率，但是任务间独立性相对较差（多个任务共享相同的 JVM）。

- `parallelism.default`：如果没有在任何地方指定并行度，则使用这个默认并行度（默认值 1）。

##### 1.4、检查点设置


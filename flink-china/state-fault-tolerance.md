### 状态管理与容错机制

#### 1、状态管理

##### 1.1、状态的概念

##### 1.1.1、无状态计算的例子：消费延迟计算

![1642593334939](/assets/1642593334939.png)

假设现在有一个消息队列，消息队列中有一个生产者持续往消费队列写入消息，多个消费者分别从消息队列中读取消息。

![1642078246675](/assets/1642078246675.png)

从图上可以看出，生产者已经写入 16 条消息，Offset 停留在 15 ；有 3 个消费者，有的消费快，而有的消费慢。消费快的已经消费了 13 条数据，消费者慢的才消费了 7、8 条数据。根据输入很容易就可以计算出每个消费者的消费延时——consumer 0 落后了 5 条，consumer 1 落后了 8 条，consumer 2 落后了 3 条。

在无状态模式计算中：

- 单条输入包含所需要的所有信息
- 相同输入可以得到相同输出

##### 1.1.2、有状态计算的例子：访问量统计

![1642593770561](/assets/1642593770561.png)

对于 Nginx 访问日志，一条日志表示一个请求，记录该请求从哪里来，访问的哪个地址。要实时统计每个地址总共被访问了多少次，统计的时间节点不同，输出结果可能不同；第一次接收到 /api/a 日志时输出 count=1 ，第二次输出 count=2。

在有状态计算模式中：

- 单条输入仅包含所需的部分信息
- 相同输入可能得到不同的输出

##### 1.1.3、需要使用状态的场景举例

![1642594317865](/assets/1642594317865.png)

- **去重**：比如上游的系统数据可能会有重复，落到下游系统时希望把重复的数据都去掉。去重需要先了解哪些数据来过，哪些数据还没有来，也就是把所有的主键都记录下来，当一条数据到来后，能够看到在主键当中是否存在。
- **窗口计算**：比如统计每分钟 Nginx 日志 API 被访问了多少次。窗口是一分钟计算一次，在窗口触发前，如 08:00 ~ 08:01 这个窗口，前 59 秒的数据来了需要先放入内存，即需要把这个窗口之内的数据先保留下来，等到 8:01 时一分钟后，再将整个窗口内触发的数据输出。未触发的窗口数据也是一种状态。
- **机器学习 / 深度学习**：如训练的模型以及当前模型的参数也是一种状态，机器学习可能每次都用有一个数据集，需要在数据集上进行学习，对模型进行一个反馈。
- **访问历史数据**：比如与昨天的数据进行对比，需要访问一些历史数据。如果每次从外部去读，对资源的消耗可能比较大，所以也希望把这些历史数据也放入状态中做对比。

##### 1.2、内存管理状态

管理状态最直接的方式就是将数据都放到内存中，这也是很常见的做法。

但是，对流式作业有以下要求：

- 7*24 小时运行，高可靠；
- 数据不丢不重，恰好计算一次；
- 数据实时产出，不延迟；

基于以上要求，内存的管理就会出现一些问题。由于内存的容量是有限制的。如果要做 24 小时的窗口计算，将 24 小时的数据都放到内存，可能会出现内存不足；另外，作业是 7*24，需要保障高可用，机器若出现故障或者宕机，需要考虑如何备份及从备份中去恢复，保证运行的作业不受影响；此外，考虑横向扩展，假如网站的访问量不高，统计每个 API 访问次数的程序可以用单线程去运行，但如果网站访问量突然增加，单节点无法处理全部访问数据，此时需要增加几个节点进行横向扩展，这时数据的状态如何平均分配到新增加的节点也问题之一。因此，将数据都放到内存中，并不是最合适的一种状态管理方式。

##### 1.3、理想的状态管理

![1642594849189](/assets/1642594849189.png)

最理想的状态管理需要满足易用、高效、可靠三点需求：

- **易用**，Flink 提供了丰富的数据结构、多样的状态组织形式以及简洁的扩展接口，让状态管理更加易用；
- **高效**，实时作业一般需要更低的延迟，一旦出现故障，恢复速度也需要更快；当处理能力不够时，可以横向扩展，同时在处理备份时，不影响作业本身处理性能；
- **可靠**，Flink 提供了状态持久化，包括不丢不重的语义以及具备自动的容错能力，比如 HA，当节点挂掉后会自动拉起，不需要人工介入。

#### 2、状态的类型与使用

##### 2.1、Managed State 和 Raw State

![1643448716003](/assets/1643448716003.png)

- 状态管理方式上：Managed State 由 Flink Runtime 管理，自动存储，自动恢复，在内存管理上有优化；而 Raw State 需要用户自己管理，需要自己序列化，Flink 不知道 State 中存储的数据结构，只有用户自己知道，需要最终序列化为可存储的数据结构。
- 状态数据结构上：Managed State 支持已知的数据结构，如 Value、List、Map 等。而 Raw State 只支持字节数组，所有状态都要转化为二进制字节数组才可以。
- 推荐使用场景：Managed State 大多数情况下均可使用，Raw State 是当 Managed State 不够用时，不如需要自定义 Operator 时推荐使用 Raw State。

##### 2.2、Keyed State 和 Operator State

Managed State 分为两种，一种是 Keyed State，另外一种是 Operator State。

![1643449243976](/assets/1643449243976.png)

Flink Stream 模型中，DataStream 经过 keyBy 操作可以转换为 KeyedStream。

每个 Key 对应一个 State，即一个 Operator 实例处理多个 Key，访问相应的多个 State，由此衍生出 Keyed State。Keyed State 只能用在 KeyedStream 的算子中。

Operator State 可以用于所有的算子，相对于数据源有一个更好的匹配方式，常用于 Source，比如 FlinkKafkaConsumer。

对于 Keyed State，一个 Operator 实例对应多个 State，随着并发的改变，Keyed State 中 State 随着 Key 在实例间迁移，比如原来有一个并发，对应的 API 请求过来，/api/a 和 /api/b 都存放在这个实例当中；如果请求量变大，需要扩容，就会把 /api/a 的状态和 /api/b 的状态分别放在不同的节点。Operator State 没有 Key，一个 Operator 实例对应一个 State，并发改变时需要选择状态如何重新分配。其中内置了 2 种分配方式：一种是均匀分配，另外一种是将所有 State 合并为全量 State 再分发给每个实例。

在访问上，Keyed State 通过 RuntimeContext 访问，需要 Operator 是一个 Rich Function。Operator State 需要自己实现 CheckpointedFunction 或者 ListCheckpointed 接口。在数据结构上，Keyed State 支持的数据结构较多，包括：ValueState、ListState、ReducingState、AggregatingState 和 MapState；而 Operator State 支持的数据结构相对较少，比如 ListState。

##### 2.3、Keyed State 使用

##### 集中 Keyed State 之间的关系

![1643510572565](/assets/1643510572565.png)

##### Keyed State 的差异：

|                  | 状态数据类型 | 访问接口                                                     |
| ---------------- | ------------ | ------------------------------------------------------------ |
| ValueState       | 单个值       | update(T) / T value()                                        |
| MapState         | Map          | put(UK key, UV value) / putAll(Map&lt;UK, UV&gt; map)<br>remove(UK key)<br>boolean contains(UK key) / UV get(UK key)<br>Iterable&lt;Map.Entry&gt; entries() / Iterator&lt;Map.Entry&gt; iterator()<br>Iterable&lt;UK&gt; keys() / Iterable&lt;UV&gt; values() |
| ListState        | List         | add(T) / addAll(List&lt;T&gt;)<br>update(List&lt;T&gt;) / Iterable&lt;T&gt; get() |
| ReducingState    | 单个值       | add(T) / addAll(List&lt;T&gt;)<br/>update(List&lt;T&gt;) / T get() |
| AggregatingState | 单个值       | add(IN) / OUT get()                                          |

- ValueState：存储单个值，比如 Wordcount，用 Word 作 Key，State 就是 Word 对应的 Count。这里面的当遏制可能是数值或者字符串，作为单个值，访问接口有两种 get 和 set，在 State 上体现的是 `update(T)`、`T value()`。
- MapState：状态数据类型是 Map，在 State 上有 put、remove 等。需要注意的是，在 MapState 中 key 和 Keyed state 中的 key 不是同一个。
- ListState：状态数据类型是 List，访问接口 add、update 等。
- ReducingState 和 AggregatingState 虽然与 ListState 都是同一个父类，但是状态数据类型是单个值，原因在于 add 方法不是把当前元素追加到列表中，而是把当前元素更新到了 Reducing 的结果中。
- AggregatingState 与 ReducingState 的区别在于，ReducingState 的 add、get 添加和得到的是同一个类型，而 AggregatingState 输入的是 IN，得到的是 OUT。

##### ValueState 使用示例

```java
// 简单状态机
// 完整代码：https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/statemachine/StateMachineExample.java

DataStream<Event> events = env.addSource(source);

DataStream<Alert> alerts = events
	.keyBy(Event::sourceAddress)
	.flatMap(new StateMachineMapper());

static class StateMachineMapper extends RichFlatMapFunction<Event, Alert> {

    // currentState 是当前状态机上的状态
    private ValueState<State> currentState;

    public void open(Configuration conf) {
        // 通过 getRuntimeContext 获取当前状态机上的状态
		currentState = getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
	}

    public void flatMap(Event evt, Collector<Alert> out) throws Exception {
        // state 是本地变量，不是 Flink 中管理的状态
		State state = currentState.value();
        // 如果 state 为 null，说明状态没有被使用过，应该是初始状态，进行初始化
		if (state == null) {
            // state 初始化
			state = State.Initial;
		}
        // 通过 transition 应用事件对 state 的影响
		State nextState = state.transition(evt.type());
        //  判断 nextState 状态是否合法
		if (nextState == State.InvalidTransition) {
			out.collect(new Alert(evt.sourceAddress(), state, evt.type()));
		} else if (nextState.isTerminal()) {
            // nextState 是最终状态，不会再发生状态改变了，执行 clear
            // clear 是所有 Flink 管理的 keyed state 都有的方法，意味着将信息删除
			currentState.clear();
		} else {
            // 对状态执行更新
			currentState.update(nextState);
		}
	}
}
```

其中，Events 是一个 DataStream，通过 `env.addSource` 加载数据，alerts 是 events 先 `keyBy` 再 `flatMap(new StateMachineMapper())` 的到的一个 DataStream。`StateMachineMapper` 是一个状态机，状态机指有不同的状态与状态间有不同的转换关系的结合，以购物过程为例：

- 首先下单，订单生成后状态为待付款，当再来一个付款成功的事件状态，订单的状态则会从待付款变为已付款代发货
- 已付款待发货的状态时收到发货事件，订单状态将会变为配送中，配送中的状态时收到签收事件，订单的状态就变为了已签收。
- 整个过程中，如果随时收到取消订单的事件，无论当前是哪个状态，最终状态都会转移到已取消，至此状态就结束了。

#### 3、容错机制与故障恢复
### 事件驱动（Event-driven）应用

#### 1、处理函数（Process Functions）

##### 1.1、简介

`ProcessFucntion`  将事件的处理与计时器（timers）和状态（state）结合起来，构成了用于构建流处理应用的强大的基础单元。这是使用 Flink 构建事件驱动应用的基础。它与 `RichFlatMapFunction` 非常像，只是多了计时器。

##### 1.2、例子

使用 `TumblingEventTimeWindow` 来计算每个司机每个小时小费的总和：

```java
// compute the sum of the tips per hour for each driver
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .window(TumblingEventTimeWindows.of(Time.hours(1)))
        .process(new AddTips());
```

使用 `KeyedProcessFunction` 做同样的事情也是相当简单的：

```java
// compute the sum of the tips per hour for each driver
DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .process(new PseudoWindow(Time.hours(1)));
```

其中被应用到键控流上的 `PseudoWindow` 是一个 `KeyedProcessFunction`，返回的结果是  `DataStream<Tuple3<Long, Long, Float>>`（与使用 Flink 的内置事件窗口的实现产生的流是相同类型）。

`PseudoWindow` 代码概要：

```java
// Compute the sum of the tips for each driver in hour-long windows.
// The keys are driverIds.
public static class PseudoWindow extends 
        KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

    private final long durationMsec;

    public PseudoWindow(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    // Called once during initialization.
    public void open(Configuration conf) {
        . . .
    }

    @Override
    // Called as each fare arrives to be processed.
    public void processElement(
            TaxiFare fare,
            Context ctx,
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        . . .
    }

    @Override
    // Called when the current watermark indicates that a window is now complete.
    public void onTimer(long timestamp, 
            OnTimerContext context, 
            Collector<Tuple3<Long, Long, Float>> out) throws Exception {

        . . .
    }
}
```

需要注意：

- 有多种类型的 ProcessFunctions —— 这里使用的是 `KeyedProcessFunction`，还有 `CoProcessFunctions`、`BroadcastProcessFunctions` 等等。
- `KeyedProcessFunction` 是 `RichFunction` 的一种。`RichFunction` 访问 `open` 和 `getRuntimeContext` 方法需要搭配托管的键控状态（managed keyed state）使用。
- 需要实现两个回调函数（callbacks）：`processElement` 和 `onTimer`。每一条进入算子的事件都会调用 `processElement` ；计时器（timers）触发时会调用 `onTimer`。这些计时器可以是事件时间计时器也可以是处理时间计时器。`processElement` 和 `onTimer` 都包含一个 `context` 对象，可以用来与 `TimerService` 交互。这两个回调函数还有一个 `Collector` 可以用来输出结果。

##### 1.2.1、`open()` 方法

```java
// Keyed, managed state, with an entry for each window, keyed by the window's end time.
// There is a separate MapState object for each driver.
private transient MapState<Long, Float> sumOfTips;

@Override
public void open(Configuration conf) {

    MapStateDescriptor<Long, Float> sumDesc =
            new MapStateDescriptor<>("sumOfTips", Long.class, Float.class);
    sumOfTips = getRuntimeContext().getMapState(sumDesc);
}
```

由于收费事件可能不是有序进入算子的，有时候在上一个小时的结果还没处理完成之前有必要处理这个小时的数据。事实上，如果水印延迟比窗口长度长很多，那么可能会同时存在许多窗口。这里的 `open()` 实现通过使用 `MapState`（每个窗口结束时间的时间戳与该窗口小费） 对此多窗口计算进行了支持。

##### 1.2.2、`processElement()` 方法

```java
public void processElement(
        TaxiFare fare,
        Context ctx,
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

    long eventTime = fare.getEventTime();
    TimerService timerService = ctx.timerService();

    if (eventTime <= timerService.currentWatermark()) {
        // This event is late; its window has already been triggered.
    } else {
        // Round up eventTime to the end of the window containing this event.
        long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

        // Schedule a callback for when the window has been completed.
        timerService.registerEventTimeTimer(endOfWindow);

        // Add this fare's tip to the running total for that window.
        Float sum = sumOfTips.get(endOfWindow);
        if (sum == null) {
            sum = 0.0F;
        }
        sum += fare.tip;
        sumOfTips.put(endOfWindow, sum);
    }
}
```

需要考虑的事情是：

- 怎么处理迟到事件（late events）？迟到（即水印之后）的事件会被删除。如果想要对迟到事件进行更好的处理，可以考虑使用旁路输出（side output）。
- 这个例子使用了一个 `MapState` —— 它的键是时间戳，并为相同的时间戳设置了一个 `Timer`。这是一个通用模式（common pattern）；当计时器触发时，查找相关的信息很容易。

##### 1.2.3、`onTimer()` 方法

```java
public void onTimer(
        long timestamp, 
        OnTimerContext context, 
        Collector<Tuple3<Long, Long, Float>> out) throws Exception {

    long driverId = context.getCurrentKey();
    // Look up the result for the hour that just ended.
    Float sumOfTips = this.sumOfTips.get(timestamp);

    Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
    out.collect(result);
    this.sumOfTips.remove(timestamp);
}
```

注意：

- 传递给 `onTimer` 的 `OnTimerContext context` 可以用来确定当前的键。
- 当前水印到达每个小时的结尾时，伪窗口（pseudo-window）会被触发，此时 `onTimer` 被调用。这里的 `onTimer` 实现将相关的条目从 `sumOfTips` 删除了，这会导致它不能应对迟到的事件。这与使用 Flink 时间窗口时将 allowedLateness 设置为 0 是等价的。

##### 1.3、性能考虑

Flink 提供的 `MapState` 和 `ListState` 类型是针对 RocksDB 进行了优化的。在可能的情况下，应该优先使用它们，而不是使用保存某种集合的 `ValueState` 对象。RocksDB 状态后端在不进行序列化（或反序列化）就可以往 `ListState` 追加数据，并且对于 `MapState`，每个键/值对都是一个单独的 RocksDB 对象，因此可以高效地访问和更新 `MapState`。

#### 2、旁路输出（Side Outputs）

##### 2.1、简介

可能需要从一个 Flink 算子获取多于一个的输出流的场合：

- 报告异常
- 报告畸形（malformed）事件
- 报告迟到事件
- 报告操作警告——比如超时的与外部服务的连接

##### 2.2、例子

旁路输出是与 `OutputTag<T>` 相关联的。这些标签（tags）具有与旁路输出的 `DataStream` 类型相同的泛型，并且它们是有名字的。

```java
private static final OutputTag<TaxiFare> lateFares = new OutputTag<TaxiFare>("lateFares") {};
```

静态变量 `lateFares` 可以在 `PseudoWindow` 的 `processElement` 方法输出迟到事件时引用：

```java
if (eventTime <= timerService.currentWatermark()) {
    // This event is late; its window has already been triggered.
    ctx.output(lateFares, fare);
} else {
    . . .
}
```

还可以在 job 的 `main` 方法中，访问这个旁路输出的流时引用：

```java
// compute the sum of the tips per hour for each driver
SingleOutputStreamOperator hourlyTips = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .process(new PseudoWindow(Time.hours(1)));

hourlyTips.getSideOutput(lateFares).print();
```

或者，可以使用具有相同名称的两个 OutputTags 来引用相同的旁路输出，但是如果这样做，它们的类型必须相同。

#### 3、结束语（Closing Remarks）

 `ProcessFunctions` 除了可以进行运算分析，还能做很多其它事情，比如释放旧的状态。使用 `KeyedProcessFunction` 和计时器可以用来检测和清除陈旧的状态。
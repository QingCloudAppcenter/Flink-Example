package examples.app;

import com.alibaba.fastjson.JSONObject;
import examples.bean.Order;
import examples.bean.OrderPriceCount;
import examples.function.UTC2Local;
import examples.utils.ArgsUtils;
import examples.utils.ClickHouseUtils;
import examples.utils.DateTimeUtil;
import examples.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TopN {
    public static void main(String[] args) throws Exception {
        final ParameterTool tool = ArgsUtils.getTool(args);
        final String brokers = tool.getRequired("kafka.brokers");
        final String topic = tool.getRequired("kafka.topics");
        final String groupId = tool.getRequired("kafka.group.id");
        final String clickhouseUrl = tool.getRequired("clickhouse.url");
        final String username = tool.getRequired("clickhouse.username");
        final String password = tool.getRequired("clickhouse.password");
        final boolean useSQL = tool.getBoolean("use.sql", false);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 2000L));
//        env.enableCheckpointing(10000L);
//        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/tmp/ck"));
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);

        DataStream<OrderPriceCount> aggStream;
        if (useSQL) {
            final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
            tableEnv.executeSql("CREATE TABLE IF NOT EXISTS orders(" +
                    "       `bidtime`   TIMESTAMP(3)," +
                    "       `price`     DOUBLE," +
                    "       `supplier`  STRING," +
                    "       WATERMARK FOR bidtime AS bidtime - INTERVAL '2' SECOND" +
                    ") with ("
                    + KafkaUtils.getKafkaDDL(brokers, topic, groupId) +
                    ")");
            final Table table = tableEnv.sqlQuery("SELECT supplier,TUMBLE_START(bidtime,INTERVAL '30' SECOND) AS stt" +
                    ",TUMBLE_END(bidtime,INTERVAL '30' SECOND) AS edt" +
                    ",SUM(price) AS price" +
                    " FROM orders" +
                    " GROUP BY TUMBLE(bidtime,INTERVAL '30' SECOND),supplier");
            tableEnv.createTemporaryFunction("utc2local", new UTC2Local());
            tableEnv.createTemporaryView("order_tmp", table);
            final Table tableTmp = tableEnv.sqlQuery("SELECT CAST(utc2local(stt) AS STRING) AS stt,CAST(utc2local(edt) AS STRING) AS edt,supplier,price FROM order_tmp");
            aggStream = tableEnv.toAppendStream(tableTmp, OrderPriceCount.class);
        } else {
            final DataStream<Order> sourceWithWatered = env.addSource(KafkaUtils.getKafkaConsumer(brokers, groupId, topic))
                    .map(json -> JSONObject.parseObject(json, Order.class))
                    .returns(Order.class)
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                            .withTimestampAssigner((element, recordTimestamp) -> DateTimeUtil.toTs(element.getBidtime())));
            final WindowedStream<Order, String, TimeWindow> windowDS = sourceWithWatered.keyBy(Order::getSupplier)
                    .window(TumblingEventTimeWindows.of(Time.seconds(30)));

            aggStream = windowDS.aggregate(new AggregateFunction<Order, Double, Double>() {
                @Override
                public Double createAccumulator() {
                    return 0D;
                }

                @Override
                public Double add(Order value, Double accumulator) {
                    return value.getPrice() + accumulator;
                }

                @Override
                public Double getResult(Double accumulator) {
                    return accumulator;
                }

                @Override
                public Double merge(Double a, Double b) {
                    return a + b;
                }
            }, new WindowFunction<Double, OrderPriceCount, String, TimeWindow>() {
                @Override
                public void apply(String supplier, TimeWindow window, Iterable<Double> iterable, Collector<OrderPriceCount> collector) throws Exception {
                    final Double price = iterable.iterator().next();
                    final long start = window.getStart();
                    final long end = window.getEnd();
                    final String startTime = DateTimeUtil.toYMDhms(new Date(start));
                    final String endTime = DateTimeUtil.toYMDhms(new Date(end));

                    collector.collect(new OrderPriceCount(price, supplier, startTime, endTime));
                }
            });
        }

        final SingleOutputStreamOperator<List<OrderPriceCount>> top3Ds = aggStream
                .keyBy(new KeySelector<OrderPriceCount, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(OrderPriceCount value) throws Exception {
                        return Tuple2.of(value.getStt(), value.getEdt());
                    }
                })
                .process(new KeyedProcessFunction<Tuple2<String, String>, OrderPriceCount, List<OrderPriceCount>>() {
                    private transient ListState<OrderPriceCount> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<OrderPriceCount>("order-state", OrderPriceCount.class));
                    }

                    @Override
                    public void processElement(OrderPriceCount orderPriceCount, KeyedProcessFunction<Tuple2<String, String>, OrderPriceCount, List<OrderPriceCount>>.Context context, Collector<List<OrderPriceCount>> collector) throws Exception {
                        listState.add(orderPriceCount);
                        context.timerService().registerEventTimeTimer(DateTimeUtil.toTs(orderPriceCount.getEdt()) + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<String, String>, OrderPriceCount, List<OrderPriceCount>>.OnTimerContext ctx, Collector<List<OrderPriceCount>> out) throws Exception {
                        final ArrayList<OrderPriceCount> list = Lists.newArrayList(listState.get());
                        list.sort((v1, v2) -> Double.compare(v2.getPrice(), v1.getPrice()));
                        ArrayList<OrderPriceCount> sortedList = new ArrayList<>();
                        for (int i = 0; i < Math.min(3, list.size()); i++) {
                            sortedList.add(list.get(i));
                        }
                        out.collect(sortedList);
                    }
                });

        top3Ds.print().setParallelism(1);
        top3Ds.flatMap((List<OrderPriceCount> list, Collector<OrderPriceCount> out) -> list.forEach(out::collect))
                .returns(OrderPriceCount.class)
                .addSink(ClickHouseUtils.getSink(clickhouseUrl, username, password, "insert into top_price(price,supplier,start,end) values (?,?,?,?)"));

        env.execute(TopN.class.getCanonicalName());
    }
}

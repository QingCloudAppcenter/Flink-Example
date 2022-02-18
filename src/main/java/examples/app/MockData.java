package examples.app;

import com.alibaba.fastjson.JSONObject;
import examples.bean.Order;
import examples.utils.ArgsUtils;
import examples.utils.FlinkFaker;
import examples.utils.KafkaUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.TimeUnit;

public class MockData {
    public static void main(String[] args) throws Exception {
        final ParameterTool tool = ArgsUtils.getTool(args);
        final String brokers = tool.getRequired("kafka.brokers");
        final String topic = tool.getRequired("kafka.topics");
        final Long generateInterval = tool.getLong("generate.interval", 200);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new RichSourceFunction<Order>() {
            private volatile boolean isCanceled;
            private transient FlinkFaker faker;

            @Override
            public void open(Configuration parameters) throws Exception {
                faker = new FlinkFaker();
            }

            @Override
            public void run(SourceContext<Order> ctx) throws Exception {
                while (!isCanceled) {
                    String time = faker.expression("#{date.past '5','1','SECONDS'}");
                    final Order order = new Order();
                    order.setSupplier(faker.expression("#{regexify '(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)'}"));
                    order.setPrice(Double.parseDouble(faker.expression("#{Number.randomDouble '2','1','150'}")));
                    order.setBidtime(time.substring(0, time.lastIndexOf(".")));
                    ctx.collect(order);
                    TimeUnit.MILLISECONDS.sleep(generateInterval);
                }
            }

            @Override
            public void cancel() {
                isCanceled = true;
            }
        }).addSink(KafkaUtils.getKafkaProducer(brokers
                , (KafkaSerializationSchema<Order>) (order, aLong) ->
                        new ProducerRecord<>(topic, JSONObject.toJSONString(order).getBytes()))).setParallelism(1);

        env.execute(MockData.class.getCanonicalName());
    }
}

package de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.processor;

import de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.common.utils.FileReader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.*;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    private static WindowAssigner<Object, TimeWindow> getWindow(String windowType, long windowSize) throws Exception {
        switch (windowType) {
            case "tumbling":
                return TumblingEventTimeWindows.of(Time.milliseconds(windowSize));
            case "sliding":
                return SlidingEventTimeWindows.of(Time.milliseconds(windowSize), Time.milliseconds(windowSize / 5));
            case "session":
                return EventTimeSessionWindows.withGap(Time.milliseconds(windowSize));
            default:
                throw new Exception("Unknown window type");
        }
    }


    public static class EventFilterBolt implements FilterFunction<AdEvent> {

        private final String et;

        public EventFilterBolt(String et) {
            this.et = et;
        }

        @Override
        public boolean filter(AdEvent adEvent) {
            return adEvent.getEt().equals(et);
        }
    }


    public static class EventMapper implements MapFunction<AdEvent, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> map(AdEvent adEvent) {
            return new Tuple2<>(adEvent.getId(), adEvent.getTs());
        }
    }


    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, Long>, Tuple3<String, String, Long>> {

        transient RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            String redisServerHostname = parameterTool.getRequired("redis.host");
            int redisServerPort = Integer.parseInt(parameterTool.getRequired("redis.port"));
            LOG.info("Opening connection with Jedis to " + redisServerHostname);
            this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHostname, redisServerPort);
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, Long> input, Collector<Tuple3<String, String, Long>> out) {

            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if (campaign_id == null) {
                campaign_id = "UNKNOWN";
            }

            out.collect(new Tuple3<>(campaign_id, ad_id, input.getField(1)));
        }
    }


    public static class JoinBolt implements JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple3<String, String, Long>> {

        @Override
        public Tuple3<String, String, Long> join(Tuple3<String, String, Long> viewed, Tuple3<String, String, Long> clicked) {
            return new Tuple3<>(viewed.getField(0), viewed.getField(1), clicked.getField(2));
        }
    }


    public static class PrepareMessageJoin implements MapFunction<Tuple3<String, String, Long>, String> {

        @Override
        public String map(Tuple3<String, String, Long> value) {
            return String.format("{campaign_id: %s, ad_id: %s, ts_clicked: %d}", value.f0, value.f1, value.f2);
        }
    }


    public static class CampaignProcessor extends ProcessWindowFunction<Tuple3<String, String, Long>, Tuple3<String, Long, Long>, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Tuple3<String, Long, Long>> out) {

            // count the number of ads for this campaign
            Iterator<Tuple3<String, String, Long>> iterator = elements.iterator();
            long count = 0;
            while (iterator.hasNext()) {
                count++;
                iterator.next();
            }

            out.collect(new Tuple3<>(key, count, context.window().getEnd()));
        }
    }


    public static class PrepareMessageAggregate implements MapFunction<Tuple3<String, Long, Long>, String> {

        @Override
        public String map(Tuple3<String, Long, Long> value) {
            return String.format("{campaign_id: %s, count: %d, window: %d}", value.f0, value.f1, value.f2);
        }
    }


    public static void main(final String[] args) throws Exception {

        if (args.length != 9) {
            throw new IllegalStateException("Required Command line argument: jobName brokerList consumerTopic producerTopic checkpointInterval windowType windowSize applyJoin applyAggregate");
        }
        String jobName = args[0];
        String brokerList = args[1];
        String consumerTopic = args[2];
        String producerTopic = args[3];
        int checkpointInterval = Integer.parseInt(args[4]);
        String windowType = args[5];
        long windowSize = Long.parseLong(args[6]);                  // IN MILLISECONDS
        boolean applyJoin = Boolean.parseBoolean(args[7]);
        boolean applyAggregate = Boolean.parseBoolean(args[8]);

        // retrieve properties from file
        Properties props = FileReader.GET.read("advertising.properties", Properties.class);

        // creating map for global properties
        Map<String, String> propsMap = new HashMap<>();
        for (final String name : props.stringPropertyNames()) {
            propsMap.put(name, props.getProperty(name));
        }

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(-1);

        // setting global properties from file
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(propsMap));

        env.disableOperatorChaining();

        // configuring RocksDB state backend to use HDFS
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(props.getProperty("hdfs.backupFolder"));

        // start a checkpoint based on supplied interval
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoints have to complete within two minutes, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(120000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        // enable externalized checkpoints which are deleted after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // no external services which could take some time to respond, therefore 1
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // KAFKA SOURCE
        KafkaSource<AdEvent> source = KafkaSource.<AdEvent>builder()
            .setBootstrapServers(brokerList)                            // Broker default host:port
            .setTopics(consumerTopic)
            .setGroupId(UUID.randomUUID().toString())                   // Consumer group ID
            .setStartingOffsets(OffsetsInitializer.latest())            // Always read topic from start
            .setValueOnlyDeserializer(new AdEventSchema())
            .build();

        // KAFKA SINK
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "3600000");
        kafkaProducerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProducerProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
        kafkaProducerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProducerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000");
        kafkaProducerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        kafkaProducerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokerList)
                .setKafkaProducerConfig(kafkaProducerProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(producerTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        // STREAM_1
        DataStream<Tuple3<String, String, Long>> viewStream = env
                .fromSource(source, WatermarkStrategy
                                .<AdEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1)),
                        // no TimestampAssigner --> the timestamps of the Kafka records will be used instead
                        "DeserializeBoltViewStream")
                .filter(new EventFilterBolt("view"))
                .name("EventFilterBoltViewStream")
                .map(new EventMapper())
                .name("EventMapperViewStream")
                .flatMap(new RedisJoinBolt())
                .name("RedisJoinBoltViewStream");

        if (applyJoin) {
            // STREAM_2
            DataStream<Tuple3<String, String, Long>> clickStream = env
                    .fromSource(source, WatermarkStrategy
                                    .<AdEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1)),
                            "DeserializeBoltClickStream")
                    .filter(new EventFilterBolt("click"))
                    .name("EventFilterBoltClickStream")
                    .map(new EventMapper())
                    .name("EventMapperClickStream")
                    .flatMap(new RedisJoinBolt())
                    .name("RedisJoinBoltClickStream");

            // JOIN STREAM_1 & STREAM_2
            viewStream = viewStream
                    .join(clickStream)
                    .where(event -> event.f1)
                    .equalTo(event -> event.f1)
                    .window(getWindow(windowType, windowSize))
                    .apply(new JoinBolt());

            if (!applyAggregate) {
                viewStream
                        .map(new PrepareMessageJoin())
                        .name("PrepareMessage")
                        .sinkTo(sink)
                        .name("KafkaSink");
            }
        }

        if (applyAggregate) {
            viewStream
                .keyBy(event -> event.f0)
                .window(getWindow(windowType, windowSize))
                .process(new CampaignProcessor())
                .name("CampaignProcessor")
                .map(new PrepareMessageAggregate())
                .name("PrepareMessage")
                .sinkTo(sink)
                .name("KafkaSink");
        }

        env.execute(jobName);
    }
}

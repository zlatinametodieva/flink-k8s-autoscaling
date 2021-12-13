package de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.common.data.TimeSeries;
import de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.common.ads.AdEvent;
import de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.common.utils.FileParser;
import de.tu_berlin.dos.arm.yahoo_streaming_benchmark_experiment.common.utils.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class Run {

    public static class PostEventSerializer implements Serializer<AdEvent> {

        private static final Logger LOG = Logger.getLogger(PostEventSerializer.class);

        private final ObjectMapper objectMap = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) { }

        @Override
        public byte[] serialize(String topic, AdEvent adEvent) {
            try {
                String msg = objectMap.writeValueAsString(adEvent);
                return msg.getBytes();
            }
            catch(JsonProcessingException ex) {
                LOG.error("Error in Serialization", ex);
            }
            return null;
        }

        @Override
        public void close() { }
    }

    public static void main(String[] args) throws Exception {

        // get properties file
        Properties producerProps = FileReader.GET.read("producer.properties", Properties.class);

        String fileName = producerProps.getProperty("dataset.fileName");
        File file = FileReader.GET.read(fileName, File.class);
        TimeSeries ts = new TimeSeries(FileParser.GET.csv(file, "\\|", true), 86400);
        int largest = Integer.parseInt(producerProps.getProperty("dataset.largest"));

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", producerProps.getProperty("kafka.brokerList"));
        kafkaProps.put("acks", "0");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1000);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", PostEventSerializer.class.getName());

        KafkaProducer<String, AdEvent> kafkaProducer = new KafkaProducer<>(kafkaProps);

        // initialize jedis
        String redisHost = producerProps.getProperty("redis.host");
        int redisPort = Integer.parseInt(producerProps.getProperty("redis.port"));
        JedisPool pool = new JedisPool(redisHost, redisPort);
        Jedis jedis = pool.getResource();

        // reset redis
        jedis.flushAll();

        // write campaign ids
        jedis.sadd("campaigns", Ads.campaignIds);

        // assign 10 ads to each campaign
        System.out.println("Writing ads to redis...");
        for (int i = 0; i < Ads.campaignIds.length; i++) {
            for (int j = 0; j < 10; j++) {
                jedis.set(Ads.adIds[i*10+j], Ads.campaignIds[i]);
            }
        }

        // initialize generation of ad events
        System.out.println("Generating ads...");
        Generator.GET.generate(ts, largest, producerProps.getProperty("kafka.topic"), kafkaProducer);
    }
}

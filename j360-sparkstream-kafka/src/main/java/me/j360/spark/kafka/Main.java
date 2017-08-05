package me.j360.spark.kafka;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.serializer.StringDecoder;
import me.j360.spark.kafka.util.redis.JedisUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Package: me.j360.spark.kafka
 * User: min_xu
 * Date: 2017/8/5 下午1:12
 * 说明：
 */
public class Main {

    private static final Pattern SPACE = Pattern.compile(" ");
    private static final String ipAddr = "123.59.27.210";
    private static final int port = 6379;
    private static Jedis jedis= null;
    private static String DMPREDIS = "tv:vp:dmp:%d";

    private Main() {
    }


    public static void main(String[] args) throws Exception {
        jedis = JedisUtil.getInstance().getJedis(ipAddr, port);
        directKafkaDMP(args);
    }


    public static void kafka(String[] args)  throws Exception{
        if (args.length < 4) {
            System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        //StreamingExamples.setStreamingLogLevels();
        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(Tuple2::_2);

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }


    /**
     * 需要自己去维护offset,性能更好
     * 本次可以重复测试需要
     * @param args
     * @throws Exception
     */
    public static void directKafka(String[] args) throws Exception {
        /*if (args.length < 2) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        //StreamingExamples.setStreamingLogLevels();

        String brokers = args[0];
        String topics = args[1];*/

        String brokers = "localhost:9092";
        String topics = "dmp";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(Tuple2::_2);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }


    /**
     * 需要自己控制offset,使用jedis控制
     * http://www.cnblogs.com/itboys/p/6036376.html
     * http://blog.csdn.net/xiao_jun_0820/article/details/46911775
     * @param args
     * @throws Exception
     */
    public static void directKafkaDMP(String[] args) throws Exception {
        /*if (args.length < 2) {
            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        //StreamingExamples.setStreamingLogLevels();

        String brokers = args[0];
        String topics = args[1];*/

        String brokers = "localhost:9092";
        String topics = "dmp";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        //{"uid":"068b746ed4620d25e26055a9f804385f","device_id":"068b746ed4620d25e26055a9f804385f","vp_id":1,"event_time":"1430204612405","os_type":"Android","play_count":6}
        // Get the lines, split them into words, count the words and print

        JavaDStream<PlayCount> events = messages.map(line -> {
            JsonObject resultObject = new JsonParser().parse(line._2()).getAsJsonObject();
            Long vpId = resultObject.get("vp_id").getAsLong();
            int count = resultObject.get("play_count").getAsInt();
            return new PlayCount(vpId, count);
        });

        JavaPairDStream<Long, Integer> playStream = events.mapToPair(new IpTuple());
        JavaPairDStream<Long, Integer> playCountStream = playStream.reduceByKey((i1, i2) -> i1 + i2);

        playCountStream.foreachRDD( rdd -> {
            rdd.foreachPartition( partRecords -> {
                partRecords.forEachRemaining( pair -> {
                    Long vpId = pair._1();
                    Integer count = pair._2();

                    //写入到redis
                    String key = String.format(DMPREDIS, vpId);
                    jedis.incrBy(key, count);
                });
            });
        });

        // Hold a reference to the current offset ranges, so it can be used downstream
        /*AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        messages.transformToPair(rdd -> {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            offsetRanges.set(offsets);
            return rdd;
        }).map(

        ).foreachRDD(rdd -> {
            for (OffsetRange o : offsetRanges.get()) {
                System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            }
        });*/

        //playCountStream.print();
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    public static final class IpTuple implements PairFunction< PlayCount, Long, Integer> {
        public Tuple2< Long, Integer> call( PlayCount playCount) { return new Tuple2<>( playCount.getVpId(), playCount.getCount()); }
    }


    public static void kafkaDMP(String[] args)  throws Exception{

        String zkbroker = "localhost:2181";
        String topicss = "dmp-1";
        String consumer = "group_id_0";
        int numThreads = 1;

        //StreamingExamples.setStreamingLogLevels();
        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
        // Create the context with 2 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Map<String, Integer> topicMap = new HashMap<>();
        topicMap.put(topicss, numThreads);

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkbroker, consumer, topicMap);

        JavaDStream<PlayCount> events = messages.map(line -> {
            JsonObject resultObject = new JsonParser().parse(line._2()).getAsJsonObject();
            Long vpId = resultObject.get("vp_id").getAsLong();
            int count = resultObject.get("play_count").getAsInt();
            return new PlayCount(vpId, count);
        });

        JavaPairDStream<Long, Integer> playStream = events.mapToPair(new IpTuple());
        JavaPairDStream<Long, Integer> playCountStream = playStream.reduceByKey((i1, i2) -> i1 + i2);

        playCountStream.foreachRDD( rdd -> {
            rdd.foreachPartition( partRecords -> {
                partRecords.forEachRemaining( pair -> {
                    Long vpId = pair._1();
                    Integer count = pair._2();

                    //写入到redis
                    String key = String.format(DMPREDIS, vpId);
                    jedis.incrBy(key, count);
                });
            });
        });

        jssc.start();
        jssc.awaitTermination();
    }

}


package me.j360.spark.kafka;

/**
 * Package: me.j360.spark.kafka
 * User: min_xu
 * Date: 2017/8/7 上午11:45
 * 说明：
 */

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import me.j360.spark.kafka.util.redis.JedisUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import redis.clients.jedis.Jedis;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters$;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KafkaOffsetExample
 */
public class KafkaOffsetExample {

    private static KafkaCluster kafkaCluster = null;

    private static HashMap<String, String> kafkaParam = new HashMap<String, String>();

    private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;

    private static scala.collection.immutable.Set<String> immutableTopics = null;


    private static final String ipAddr = "123.59.27.210";
    private static final int port = 6379;
    private static Jedis jedis= null;
    private static String DMPREDIS = "tv:vp:dmp:%d";

    public static void main(String[] args) throws InterruptedException {
        jedis = JedisUtil.getInstance().getJedis(ipAddr, port);

        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
        Set<String> topicSet = new HashSet<String>();
        topicSet.add("dmp");

        kafkaParam.put("metadata.broker.list", "localhost:9092");
        kafkaParam.put("group.id", "me.j360.kafka");

        // transform java Map to scala immutable.map
        //scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParam);
        scala.collection.immutable.Map<String, String> scalaKafkaParam = convert(kafkaParam);

        // init KafkaCluster
        kafkaCluster = new KafkaCluster(scalaKafkaParam);

        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        immutableTopics = mutableTopics.toSet();
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet2 = kafkaCluster.getPartitions(immutableTopics).right().get();

        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();

        // 没有保存offset时（该group首次消费时）, 各个partition offset 默认为0
        if (kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).isLeft()) {

            System.out.println(kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).left().get());

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }

        }
        // offset已存在, 使用保存的offset
        else {

            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet2).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet2);

            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long)consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }

        }

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
        kafkaParamBroadcast = jssc.sparkContext().broadcast(kafkaParam);

        // create direct stream
        JavaInputDStream<String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParam,
                consumerOffsetsLong,
                v1 -> v1.message()

        );


        /*new Function<MessageAndMetadata<String, String>, String>() {
            public String call(MessageAndMetadata<String, String> v1) throws Exception {
                return v1.message();
            }
        }*/

        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> javaDStream = messages.transform(rdd -> {
            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            offsetRanges.set(offsets);

            return rdd;
        });


        JavaDStream<PlayCount> events = messages.map(line -> {
            JsonObject resultObject = new JsonParser().parse(line).getAsJsonObject();
            Long vpId = resultObject.get("vp_id").getAsLong();
            int count = resultObject.get("play_count").getAsInt();
            return new PlayCount(vpId, count);
        });

        JavaPairDStream<Long, Integer> playStream = events.mapToPair(new Main.IpTuple());
        JavaPairDStream<Long, Integer> playCountStream = playStream.reduceByKey((i1, i2) -> i1 + i2);

        // output
        javaDStream.foreachRDD(rdd -> {
            for (OffsetRange o : offsetRanges.get()) {
                // 封装topic.partition 与 offset对应关系 java Map
                TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
                Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
                topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

                // 转换java map to scala immutable.map
                //scala.collection.mutable.Map<TopicAndPartition, Object> testMap = JavaConversions.mapAsScalaMap(topicAndPartitionObjectMap);
                scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap = convert(topicAndPartitionObjectMap);


                //处理消息


                // 更新offset到kafkaCluster
                kafkaCluster.setConsumerOffsets(kafkaParamBroadcast.getValue().get("group.id"), scalatopicAndPartitionObjectMap);

                System.out.println(
                            o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                );
            }
        });

        //写入redis
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


    /**
     * Getting an immutable Scala map is a little tricky because the conversions provided by the collections library return all return mutable ones, and you can't just use toMap because it needs an implicit argument that the Java compiler of course won't provide. A complete solution with that implicit argument looks like this
     * @param m
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> scala.collection.immutable.Map<K, V> convert(Map<K, V> m) {
        return JavaConverters$.MODULE$.mapAsScalaMapConverter(m).asScala().toMap(
                scala.Predef$.MODULE$.<scala.Tuple2<K, V>>$conforms()
        );
    }



}

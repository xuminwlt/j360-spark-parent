package me.j360.spark.kafka.bootstrap;

import kafka.common.TopicAndPartition;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters$;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Package: me.j360.spark.kafka.bootstrap
 * User: min_xu
 * Date: 2017/8/7 下午2:40
 * 说明：
 */
public class KafkaManager {

    private KafkaCluster kafkaCluster = null;
    private java.util.Map<kafka.common.TopicAndPartition, Long> fromOffsets = new java.util.HashMap<kafka.common.TopicAndPartition, Long>();
    private static final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
    private static HashMap<String, String> kafkaParams = new HashMap<String, String>();

    private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;

    public KafkaManager(HashMap<String, String> kafkaParams) {
        this.kafkaParams = kafkaParams;
        scala.collection.immutable.Map<String, String> immutableKafkaParam = KafkaManager.convert(kafkaParams);
        kafkaCluster = new KafkaCluster(immutableKafkaParam);
    }



    public JavaInputDStream<String> createDirectStream(JavaStreamingContext jsctx, Set<String> topics) {

        setOrUpdateOffsets(topics, kafkaParams);
        kafkaParamBroadcast = jsctx.sparkContext().broadcast(kafkaParams);

        JavaInputDStream<String> stream = KafkaUtils.createDirectStream(jsctx,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams,
                this.fromOffsets,
                v1 -> v1.message());

        return stream;
    }


    public void setOrUpdateOffsets(Set<String> topics,Map<String, String> kafkaParams) {
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions
                .asScalaSet(topics);
        scala.collection.immutable.Set<String> immutableTopics = mutableTopics
                .toSet();
        scala.collection.immutable.Set<TopicAndPartition> scalaTopicAndPartitionSet = kafkaCluster
                .getPartitions(immutableTopics).right().get();

        // 首次消费，默认设置为0
        if (kafkaCluster.getConsumerOffsets(kafkaParams.get("group.id"),
                scalaTopicAndPartitionSet).isLeft()) {
            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions
                    .setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                this.fromOffsets.put(topicAndPartition, 0L);
            }
        } else {
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster
                    .getConsumerOffsets(kafkaParams.get("group.id"),
                            scalaTopicAndPartitionSet).right().get();

            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions
                    .mapAsJavaMap(consumerOffsetsTemp);
            Set<TopicAndPartition> javaTopicAndPartitionSet = JavaConversions
                    .setAsJavaSet(scalaTopicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : javaTopicAndPartitionSet) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                this.fromOffsets.put(topicAndPartition, offset);
            }
        }
    }


    public void updateZKOffsets(JavaDStream rdd) {
        rdd.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> arg0) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) arg0.rdd()).offsetRanges();
                for(OffsetRange o: offsets){
                    // 封装topic.partition 与 offset对应关系 java Map
                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());

                    // 转换java map to scala immutable.map
                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap =
                            convert(topicAndPartitionObjectMap);

                    // 更新offset到kafkaCluster
                    kafkaCluster.setConsumerOffsets(Constant.groupId, scalatopicAndPartitionObjectMap);
                }
            }
        });
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

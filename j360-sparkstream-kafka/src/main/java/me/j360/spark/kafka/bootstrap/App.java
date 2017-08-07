package me.j360.spark.kafka.bootstrap;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import me.j360.spark.kafka.Main;
import me.j360.spark.kafka.PlayCount;
import me.j360.spark.kafka.util.redis.JedisUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Package: me.j360.spark.kafka.bootstrap
 * User: min_xu
 * Date: 2017/8/7 下午2:40
 * 说明：
 */
public class App {

    private KafkaManager kafkaManager = null;
    private Set<String> topics = new HashSet<String>();
    private Duration duration = new Duration(Constant.duration);
    private static Jedis jedis= null;

    public App() {
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", Constant.brokerlist);
        kafkaParams.put("group.id", Constant.groupId);

        this.jedis = JedisUtil.getInstance().getJedis(Constant.ipAddr, Constant.port);
        this.kafkaManager = new KafkaManager(kafkaParams);
        this.topics.add(Constant.topic);
    }


    public void startApp() throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName(Constant.appName);
        JavaStreamingContext jsctx = new JavaStreamingContext(sparkConf, duration);

        JavaInputDStream<String> messages = kafkaManager.createDirectStream(jsctx, topics);
        process(messages);

        kafkaManager.updateZKOffsets(messages);

        jsctx.start();
        jsctx.awaitTermination();
    }


    public static void process(JavaInputDStream<String> messages) {

        messages.map(line -> {
            JsonObject resultObject = new JsonParser().parse(line).getAsJsonObject();
            Long vpId = resultObject.get("vp_id").getAsLong();
            int count = resultObject.get("play_count").getAsInt();
            return new PlayCount(vpId, count);
        })
                .mapToPair(new Main.IpTuple())
                .reduceByKey((i1, i2) -> i1 + i2)
                .foreachRDD(rdd -> {
                    rdd.foreachPartition( partRecords -> {
                        partRecords.forEachRemaining( pair -> {
                            Long vpId = pair._1();
                            Integer count = pair._2();

                            //写入到redis
                            String key = String.format(Constant.DMPREDIS, vpId);
                            jedis.incrBy(key, count);
                        });
                    });
                });

    }

    public static void main(String[] args) throws InterruptedException {
        App app = new App();
        app.startApp();
    }
}

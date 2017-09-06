package me.j360.spark.kafka.bootstrap;

/**
 * Package: me.j360.spark.kafka.bootstrap
 * User: min_xu
 * Date: 2017/8/7 下午2:56
 * 说明：
 */
public class Constant {

    public static String topic = "dmp";
    public static String appName = "app-dmp";
    public static long duration = 10000;

    public static String DMP_VIEW_COUNT = "view:count:%d";
    public static String DMP_VIEW_NOTICE = "view:flag:%d";

    public static int viewCountNoticeLimit = 10;

    public static String groupId = "group-dmp";
    public static int partitions = 10;
    public static final String mqDestimate = "mq.message";

    //test-profile
    public static String brokerlist = "localhost:9092";
    public static final String redisIpAddr = "10.137.195.190";
    public static final int redisPort = 6379;
    public static final String mqBrokerURL= "failover://(tcp://localhost:61616,tcp://localhost:62616)?randomize=false&initialReconnectDelay=100&timeout=50005000";
    public static final String SPARK_MASTER = "spark://localhost:7077";

}

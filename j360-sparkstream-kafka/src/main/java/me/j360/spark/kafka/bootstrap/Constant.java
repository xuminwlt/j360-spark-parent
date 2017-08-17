package me.j360.spark.kafka.bootstrap;

/**
 * Package: me.j360.spark.kafka.bootstrap
 * User: min_xu
 * Date: 2017/8/7 下午2:56
 * 说明：
 */
public class Constant {

    public static String topic = "dmp";
    public static String appName = "dmp";
    public static long duration = 10000;
    public static String brokerlist = "localhost:9092";
    public static String groupId = "me.j360.spark";
    public static int partitions = 10;

    public static final String ipAddr = "localhost";
    public static final int port = 6379;
    public static String DMPREDIS = "tv:vp:dmp:%d";

}

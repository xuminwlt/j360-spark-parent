package me.j360.spark.spring.domain;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Package: cn.paomiantv.spark.streaming.domain
 * User: min_xu
 * Date: 2017/8/7 下午6:21
 * 说明：
 */
public final class PlayTuple implements PairFunction< PlayCount, Long, Integer> {
    public Tuple2< Long, Integer> call(PlayCount playCount) { return new Tuple2<>( playCount.getVpId(), playCount.getCount()); }
}

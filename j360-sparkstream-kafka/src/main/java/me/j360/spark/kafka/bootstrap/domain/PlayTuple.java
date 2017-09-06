package me.j360.spark.kafka.bootstrap.domain;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public final class PlayTuple implements PairFunction< PlayCount, Long, Integer> {
    public Tuple2< Long, Integer> call(PlayCount playCount) { return new Tuple2<>( playCount.getVpId(), playCount.getCount()); }
}

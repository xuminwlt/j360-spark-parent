package me.j360.spark.kafka;

/**
 * Package: me.j360.spark.kafka
 * User: min_xu
 * Date: 2017/8/5 下午3:42
 * 说明：
 */


public class PlayCount {

    public PlayCount(Long vpId, int count){
        this.vpId = vpId;
        this.count = count;
    }

    private Long vpId;

    private int count;

    public Long getVpId() {
        return vpId;
    }

    public int getCount() {
        return count;
    }
}

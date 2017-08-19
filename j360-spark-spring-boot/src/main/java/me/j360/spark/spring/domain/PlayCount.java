package me.j360.spark.spring.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Package: cn.paomiantv.spark.streaming.domain
 * User: min_xu
 * Date: 2017/8/7 下午6:19
 * 说明：
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlayCount {

    private Long vpId;

    private int count;

}

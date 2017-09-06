package me.j360.spark.kafka.bootstrap.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlayCount {

    private Long vpId;
    private int count;

}

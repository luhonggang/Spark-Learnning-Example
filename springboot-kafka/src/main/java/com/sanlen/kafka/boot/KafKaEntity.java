package com.sanlen.kafka.boot;

import lombok.Data;

/**
 * @author: LuHongGang
 * @time: 2017/10/31
 * @version: 1.0
 */
@Data
public class KafKaEntity {
    private String topic;
    private String key;
    private KafKaMsg data;
}

package com.sanlen.kafka.common;

/**
 * @author: LuHongGang
 * @time: 2017/10/31
 * @version: 1.0
 */
public enum Mark {
    COMMA(",","逗号"),
    SEMICOLON(";","分号"),
    COLON(":","冒号");

    private  String code;
    private  String value;
    Mark(String code,String value) {
        this.code = code;
        this.value = value;
    }

    public String getCode(){
        return this.code;
    }
    public String getValue(){
        return this.value;
    }
}

package com.sanlen.kafka.common;

/**
 * 封装发送到KafKa的数据的key键值
 * @author: LuHongGang
 * @time: 2017/10/31
 * @version: 1.0
 */
public class KeyUtils {
    public static String autoKeys(Long prefix,String suffix){
        return  prefix+"_"+suffix;
    }

    /**
     *  自动组建成Key Value键值对的形式 以";"分开
     *  @return
     */
    public static String autoKeysValues(){

        return "";
    }
}

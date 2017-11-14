package com.sanlen.kafka.boot;
import lombok.Data;
import lombok.EqualsAndHashCode;
/**
 * @author: LuHongGang
 * @time: 2017/10/31
 * @version: 1.0
 */
@Data
@EqualsAndHashCode
public class KafKaMsg {
    /** ip地址**/
    public String ip;
    /** 浏览器的类型**/
    public String browser;
    /** 来源的平台**/
    public String source;
    /** 请求的URL**/
    public String url;
    /** 请求的方式**/
    public String requestType;
}

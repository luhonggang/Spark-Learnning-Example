package com.streaming.localhandle

import java.util

import com.streaming.utils.AccessIpAddressUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  *  模拟日志产生
  * author : LuHongGang
  * time   : 2017/11/17
  * version: 1.0
  */
object UserLogProducer {
  def main(args: Array[String]): Unit = {
    val topic = "test"
    val brokers = "Hive-EL67-0001:9092"
    val messagesPerSec=1    //每秒发送几条信息
    val wordsPerMessage =1  //一条信息包括多少个单词
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    // Send some messages
    // 2015-11-11T15:00:01|~|xxx|~|125.119.144.252|~|xxx|~|4E3512790001039FDB9
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => timeIpCookies(0))
          .mkString("|~|")
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
        println(message)
      }
      Thread.sleep(2000)
    }
  }

  def timeIpCookies(days:Int):String={
    val str = AccessIpAddressUtil.getDate(days)+"|~|"+"xxx"+"|~|"+AccessIpAddressUtil.getIp+"|~|"+"xxx"+"|~|"+AccessIpAddressUtil.getUUID()
    str
  }
}

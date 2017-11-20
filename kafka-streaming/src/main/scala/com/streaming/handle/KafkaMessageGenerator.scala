package com.streaming.handle
import java.util.{Properties, Random, UUID}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.codehaus.jettison.json.JSONObject

/** 生产数据(模拟)
  * @ author: LuHongGang
  * @ time:   2017/11/14
  * @ version: 1.0
  */
object KafkaMessageGenerator {
  private val random = new Random()
  private var pointer = -1
  private val os_type = Array(
    "Android", "IPhone OS",
    "None", "Windows Phone")

  def click() : Double = {
    random.nextInt(10)
  }

  def getOsType() : String = {
    pointer = pointer + 1
    if(pointer >= os_type.length) {
      pointer = 0
      os_type(pointer)
    } else {
      os_type(pointer)
    }
  }

  def main(args: Array[String]): Unit = {
    val topic = "user_events"
    //本地虚拟机ZK地址  生产者监听端口9092
    val brokers = "Hive-EL67-0001:9092,Hive-EL67-0002:9092,Kafka-EL67-0001:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String, String](kafkaConfig)

    while(true) {
      // prepare event data
      val event = new JSONObject()
      event
        .put("uid", UUID.randomUUID())//随机生成用户id
        .put("event_time", System.currentTimeMillis.toString) //记录时间发生时间
        .put("os_type", getOsType) //设备类型
        .put("click_count", click) //点击次数

      // produce event message
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)

      Thread.sleep(2000)
    }
  }
}

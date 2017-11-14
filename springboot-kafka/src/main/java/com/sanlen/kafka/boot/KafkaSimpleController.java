package com.sanlen.kafka.boot;

import com.sanlen.kafka.common.KeyUtils;
import com.sanlen.kafka.common.Mark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Field;

/**
 * KafKa控制器路由
 */
@RestController
@RequestMapping("/api")
public class KafkaSimpleController {

    public static Logger logger = LoggerFactory.getLogger(KafkaSimpleController.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 接收传递的JSON格式数据,并向指定的topic中输出数据
     * @param entity 参数实体
     * @return       状态信息
     * @throws ClassNotFoundException 类找不到异常
     */
    @RequestMapping(value = "/sendmessages", method = RequestMethod.POST)
    public String sendmessages(@RequestBody KafKaEntity entity) throws ClassNotFoundException {
        logger.info("获取的参数");
        if(null != entity){
            KafKaMsg data = entity.getData();
            String keys = data.getUrl();
            Long sysTime = System.currentTimeMillis();
            Class classForMsg = (Class)data.getClass();
            Field[] fields = classForMsg.getDeclaredFields();
            for (int i = 0; i <fields.length ; i++) {
                Field fs = fields[i];
                /*设置些属性是可以访问的*/
                fs.setAccessible(true);
                try {
                    logger.info("获取的字段名称是 : "+fs.getName() +" 字段值 : "+fs.get(data).toString());
                    kafkaTemplate.send(entity.getTopic(), fs.getName(), KeyUtils.autoKeys(sysTime,keys)+ Mark.COLON.getCode()+fs.get(data).toString());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        } else {
          logger.info("============>>参数为空<<============");
        }
        return "success";
    }

    /**
     * 发送单个数据
     * @param id 参数
     */
    @RequestMapping(value = "/sendstr/{id}", method = RequestMethod.GET)
    public void sendStr(@PathVariable("id") Long id){
        System.out.println("---------->发送单个数据<----------");
        kafkaTemplate.send("test",id+"");
        logger.info("=============>>成功发送单个数据<<============");
    }

    @KafkaListener(id = "t1", topics = "t1")
    public void listenT1(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("{} - {} : {}", cr.topic(), cr.key(), cr.value());
    }

    //    @KafkaListeners({
//            @KafkaListener(id = "t1-2", topics = "t1"),
//            @KafkaListener(id = "t2", topics = "t2"),
//    })
    @KafkaListener(id = "t2", topics = "t2")
    public void listenT2(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info("{} - {} : {}", cr.topic(), cr.key(), cr.value());
    }

}

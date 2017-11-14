package com.sanlen.kafka;

import com.sanlen.kafka.common.Mark;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author: LuHongGang
 * @time: 2017/10/31
 * @version: 1.0
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class CommonsTest {

    /**
     * 测试枚举获取值
     */
    @Test
    public void testEnum(){
        System.out.println(Mark.COLON.getCode());
    }

}

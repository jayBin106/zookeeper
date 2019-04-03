package com.demo.zookeeper.job;

import com.demo.zookeeper.统一配置管理.PropertyUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * TimeBomb
 * <p>
 * liwenbin
 * 2019/3/29 16:55
 */
@Component
public class TimeBomb {
    private static final Log log = LogFactory.getLog(TimeBomb.class);
    @Value("${spring.profiles.active}")
    private String active;
    @Value("${spring.redis.port}")
    private String yport;

    DateFormat dateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒");

    /**
     * 残值预估统计
     */

    @Scheduled(cron = "0/10 * * * * ? ")
    private void get() {
        if (!active.equals("dev")) {
            String host = PropertyUtils.get("spring.redis.host");
            String port = PropertyUtils.get("spring.redis.port");
            System.out.println("host：" + host);
            System.out.println("port：" + port);
            System.out.println("yport：" + yport);
            Map<String, String> getall = PropertyUtils.getall();
            for (String key : getall.keySet()) {
                String vale = getall.get(key);
                System.out.println(key + "-------" + vale);
            }
            log.info(dateFormat.format(new Date()) + "——定时任务，残值预估统计.......");
        }
    }


}

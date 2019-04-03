package com.demo.zookeeper;


import com.demo.zookeeper.统一配置管理.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.LinkedHashMap;
import java.util.Properties;

@SpringBootApplication
@EnableScheduling //定时任务注解
public class ZookeeperApplication {
    @Autowired
    private PropertyUtils propertyUtils;

    public static void main(String[] args) {
        SpringApplication.run(ZookeeperApplication.class, args);
    }

    @Bean
    public String changeEnvironment(ApplicationContext applicationContext) {
        //支持Environment获取  修改容器里面StandardServletEnvironment
        StandardEnvironment standardEnvironment = applicationContext.getBean(StandardEnvironment.class);
        //根据本地loadPropFromRemote的参数是否为true来确定是否要从远程zookeeper加载配置数据
        if (StringUtils.isNotEmpty(standardEnvironment.getProperty("loadPropFromRemote")) && "true".equals(standardEnvironment.getProperty("loadPropFromRemote"))) {
            //获取application.properties的配置信息，放入缓存中去
            Object object = standardEnvironment.getPropertySources().get("applicationConfig: [classpath:/application.properties]").getSource();
            if (object instanceof Properties) {
                propertyUtils.cacheProperties((Properties) object);
            } else if (object instanceof LinkedHashMap) {
                propertyUtils.cacheProperties((LinkedHashMap) object);
            }
            //将zookeeper中的配置信息加载到缓存中去
//            propertyUtils.loadPropertiesFromZookeeper();
            propertyUtils.initMethod();
            //弃用
//            Properties newproperties = propertyUtils.loadProperties(standardEnvironment);
//            MutablePropertySources propertySources = standardEnvironment.getPropertySources();
//            propertySources.replace("applicationConfig: [classpath:/application.properties]", new PropertiesPropertySource("applicationConfig: [classpath:/application.properties]", newproperties));
//            System.out.println(newproperties.toString());
        } else {
            Properties properties = (Properties) standardEnvironment.getPropertySources().get("applicationConfig: [classpath:/application.properties]").getSource();
            propertyUtils.cacheProperties(properties);
        }
        return null;
    }
}

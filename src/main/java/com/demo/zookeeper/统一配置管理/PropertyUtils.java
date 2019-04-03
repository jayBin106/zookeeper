package com.demo.zookeeper.统一配置管理;

import com.demo.zookeeper.ACL权限.AclUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 从zookeeper获取配置信息，监听zookeeper
 * 缓存配置到CACHE_PROPERTIES 供程序使用
 */
@Component
public class PropertyUtils {
    private static org.apache.logging.log4j.Logger log = LogManager.getLogger(PropertyUtils.class);
    private CuratorFramework curator;
    private TreeCache treeCache;
    private static final Map<String, String> CACHE_PROPERTIES = new ConcurrentHashMap<>();
    private String zkPath; //zookeeper根目录节点


    //从加载的配置文件中获取zookeeper地址信息
//    public Properties loadProperties(StandardEnvironment standardEnvironment) {
//        Map<String, String> props = new HashMap<>(3);
//        //从配置中获取zookeeper的地址
//        props.put("zookeeper.address", standardEnvironment.getProperty("zookeeper.address"));
//        String[] activeProfiles = standardEnvironment.getActiveProfiles();
//        String[] defaultProfiles = standardEnvironment.getDefaultProfiles();
//        loadPropertiesFromZookeeper(props);
//        return properties;
//    }

    //初始化方法
    public void initMethod() {
        initZkClient();
        getConfigData(zkPath);
        addZkListener();
    }

    //初始化zoookeeper客户端
    private void initZkClient() {
        //从缓存中获取
        String zookeeperAddress = CACHE_PROPERTIES.get("zookeeper.address");
        String sessionTimeout = CACHE_PROPERTIES.get("zookeeper.sessionTimeout");
        zkPath = CACHE_PROPERTIES.get("zookeeper.zkPath");
        if (StringUtils.isEmpty(zookeeperAddress) || StringUtils.isEmpty(sessionTimeout)) {
            throw new RuntimeException("配置项[zookeeper.address,zookeeper.sessionTimeout]不能为空");
        }
        curator = CuratorFrameworkFactory
                .builder()
                .connectString(zookeeperAddress)
                .sessionTimeoutMs(Integer.valueOf(sessionTimeout))
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curator.start();
    }

    private void getConfigData(String path) {
        try {
            List<String> list = curator.getChildren().forPath(path);
            for (String key : list) {
                String[] i = path.split("/");
                if (i.length == 4) {
                    String newStr = path + "/" + key;
                    //格式转换并加入缓存
                    codeChange(newStr);
                    continue;
                }
                String path2 = path + "/" + key;
                getConfigData(path2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //格式转换并加入缓存
    private void codeChange(String path) {
        int j = path.indexOf("/", 2);
        String substring = path.substring(j + 1);
        String newPath = substring.replace("/", ".");
        try {
            byte[] bytes = curator.getData().forPath(path);
            PropertyUtils.set(newPath, new String(bytes, "UTF-8"));
            System.out.println("-----------" + newPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * String value = new String(curator.getData().forPath(zkPath + "/" + key));
     * if (value != null && value.length() > 0) {
     * properties.put(key, value);
     * }
     */

    private void addZkListener() {
        TreeCacheListener listener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                //节点数据
                if (event.getData() != null) {
                    byte[] bytes = event.getData().getData();
                    String path = event.getData().getPath();
                    System.out.println("类型:" + event.getType());
                    System.out.println("路径:" + path);
                    System.out.println("数据:" + new String(bytes));
                    if (event.getType() == TreeCacheEvent.Type.NODE_UPDATED) {
                        log.info("远程配置信息变更，重新加载远程配置信息");
                        //格式转换并加入缓存
                        codeChange(path);
                    }
                }
            }
        };

        treeCache = new TreeCache(curator, zkPath);
        try {
            treeCache.start();
            treeCache.getListenable().addListener(listener);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 缓存配置信息
     *
     * @param
     */
    public void cacheProperties(Object obj) {
        if (obj instanceof Properties) {
            Properties p = (Properties) obj;
            for (Object key : p.keySet()) {
                Object value = p.get(key);
                if (value != null) {
                    PropertyUtils.set(key.toString(), value.toString());
                }
            }
        } else if (obj instanceof LinkedHashMap) {
            LinkedHashMap p = (LinkedHashMap) obj;
            for (Object key : p.keySet()) {
                Object value = p.get(key);
                if (value != null) {
                    PropertyUtils.set(key.toString(), value.toString());
                }
            }
        }
    }

    public static String get(String key) {
        return CACHE_PROPERTIES.get(key);
    }

    protected static Map<String, String> getProperties() {
        return CACHE_PROPERTIES;
    }

    private static void set(String key, String value) {
        CACHE_PROPERTIES.put(key, value);
    }

    /**
     * 测试缓存
     *
     * @return
     */
    public static Map<String, String> getall() {
        return CACHE_PROPERTIES;
    }

}

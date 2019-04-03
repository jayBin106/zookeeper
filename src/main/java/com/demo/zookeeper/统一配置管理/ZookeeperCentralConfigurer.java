package com.demo.zookeeper.统一配置管理;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.demo.zookeeper.zookeeper基本操作.ZookeeperDemo.zookeeperAddress;

public class ZookeeperCentralConfigurer {
    private CuratorFramework zkClient;
    private TreeCache treeCache;

    private String zkServers;
    private String zkPath;
    private int sessionTimeout;
    private Properties props;

    public ZookeeperCentralConfigurer(String zkServers, String zkPath, int sessionTimeout) throws IOException {
        this.zkServers = zkServers;
        this.zkPath = zkPath;
        this.sessionTimeout = sessionTimeout;
        this.props = new Properties();

        initZkClient();
        getConfigData();
        addZkListener();
        //等待输入
        System.in.read();
    }

    private void initZkClient() {
        zkClient = CuratorFrameworkFactory.builder().connectString(zkServers).sessionTimeoutMs(sessionTimeout)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        zkClient.start();
    }

    private void getConfigData() {
        try {
            List<String> list = zkClient.getChildren().forPath(zkPath);
            for (String key : list) {
                String value = new String(zkClient.getData().forPath(zkPath + "/" + key));
                if (value != null && value.length() > 0) {
                    props.put(key, value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addZkListener() {
        TreeCacheListener listener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println("类型:" + event.getType());
                System.out.println("数据:" + new String(event.getData().getData()));
                if (event.getType() == TreeCacheEvent.Type.NODE_UPDATED) {
                    getConfigData();
                }
            }
        };

        treeCache = new TreeCache(zkClient, zkPath);
        try {
            treeCache.start();
            treeCache.getListenable().addListener(listener);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Properties getProps() {
        return props;
    }

    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }

    public void setZkPath(String zkPath) {
        this.zkPath = zkPath;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public static void main(String[] args) {
        String path = "/disconfig";
        try {
            new ZookeeperCentralConfigurer(zookeeperAddress, path, 5000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

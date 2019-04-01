package com.demo.zookeeper.cache缓存监听;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * Cache
 * Zookeeper原生支持通过注册Watcher来进行事件监听，但是开发者需要反复注册(Watcher只能单次注册单次使用)。Cache是Curator中对事件监听的包装，
 * * 可以看作是对事件监听的本地缓存视图，能够自动为开发者处理反复注册监听。Curator提供了三种Watcher(Cache)来监听结点的变化。
 * liwenbin
 * 2019/4/1 9:43
 */
public class Cache {
    private final static String zookeeperAddress = "192.168.1.232:2181";
    CuratorFramework curatorFramework = null;

    /**
     * 创建一个会话
     */
    public Cache() {
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
        /**
         * 参数名	说明
         * connectionString	服务器列表，格式host1:port1,host2:port2,…
         * retryPolicy	重试策略,内建有四种重试策略,也可以自行实现RetryPolicy接口
         * sessionTimeoutMs	会话超时时间，单位毫秒，默认60000ms
         * connectionTimeoutMs	连接创建超时时间，单位毫秒，默认60000ms
         */
        // 1.使用静态工程方法创建客户端
//        curatorFramework = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);


        //2,使用Fluent风格的Api创建会话
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zookeeperAddress)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .retryPolicy(retry)
                .namespace("jay")   //创建命名空间
                .build();


        //启动客户端
        curatorFramework.start();
    }


    /**
     * <p>
     * Path Cache
     * Path Cache用来监控一个ZNode的子节点. 当一个子节点增加， 更新，删除时， Path Cache会改变它的状态，
     * 会包含最新的子节点， 子节点的数据和状态，而状态的更变将通过PathChildrenCacheListener通知。
     * 只能监听子节点信息，不能监听子节点下的节点的更新变化！！！！！！！
     *
     * 实际使用时会涉及到四个类：
     *
     * PathChildrenCache
     * PathChildrenCacheEvent
     * PathChildrenCacheListener
     * ChildData
     */

    @Test
    public void pathCache() throws Exception {
        String PATHCACHE = "/cache/pathCache";

        //创建pathcache true 表示缓存节点信息，如果为false说，even.getData将为null
        PathChildrenCache cache = new PathChildrenCache(curatorFramework, PATHCACHE, true);
        cache.start();
        //创建监听
        PathChildrenCacheListener pathChildrenCacheListener = (client2, event) -> {
            System.out.println("事件类型：" + event.getType());
            if (event.getData() != null) {
                System.out.println("节点路径：" + event.getData().getPath() + "----节点数据：" + new String(event.getData().getData()));
            }
        };
        //开始监听
        cache.getListenable().addListener(pathChildrenCacheListener);

        //创建节点
        String forPath = curatorFramework
                .create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(PATHCACHE + "/c", "12".getBytes());
        System.out.println("创建节点--" + forPath);
//        //创建节点
        String forPath2 = curatorFramework
                .create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(PATHCACHE + "/c/a", "12".getBytes());
        System.out.println("创建节点--" + forPath2);

        //更新节点
        curatorFramework
                .setData()
                .forPath(PATHCACHE + "/c", "123".getBytes());
        //更新节点
        curatorFramework
                .setData()
                .forPath(PATHCACHE + "/c/a", "123".getBytes());

        //删除节点
//        curatorFramework
//                .delete()
//                .deletingChildrenIfNeeded()
//                .forPath(PATHCACHE);
        //阻塞。等待输入
        System.in.read();
    }


    /**
     * node cache  只是监听某一个特定的节点变化
     */
    @Test
    public void nodeCache() throws Exception {
        String nodeCacheStr = "/cache/nodeCache";

        //创建nodecache
//        String s = curatorFramework
//                .create()
//                .creatingParentContainersIfNeeded()
//                .withMode(CreateMode.PERSISTENT)
//                .forPath(nodeCacheStr, "1".getBytes());
//        System.out.println("1111111创建节点" + s);

        NodeCache nodeCache = new NodeCache(curatorFramework, nodeCacheStr);
        NodeCacheListener nodeCacheListener = () -> {
            ChildData currentData = nodeCache.getCurrentData();
            if (currentData != null) {
                System.out.println("节点数据：" + new String(currentData.getData()));
            } else {
                System.out.println("节点不存在。。");
            }
        };
        //加入监听
        nodeCache.getListenable().addListener(nodeCacheListener);
        //开始监听
        nodeCache.start();

//        String s1 = curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(nodeCacheStr, "1".getBytes());
//        System.out.println("创建节点" + s1);

        byte[] bytes = curatorFramework.getData().forPath(nodeCacheStr);
        System.out.println("节点值：" + new String(bytes));

        Stat stat = curatorFramework.setData().forPath(nodeCacheStr, "112".getBytes());
        System.out.println("节点状态：" + stat);

//        Void aVoid = curatorFramework.delete().deletingChildrenIfNeeded().forPath(nodeCacheStr);
//        System.out.println("删除节点");

        //阻塞。等待输入
        System.in.read();
    }


    /**
     * Tree Cache
     * Tree Cache可以监控整个树上的所有节点，类似于PathCache和NodeCache的组合，主要涉及到下面四个类：
     * <p>
     * TreeCache - Tree Cache实现类
     * TreeCacheListener - 监听器类
     * TreeCacheEvent - 触发的事件类
     * ChildData - 节点数据
     *
     * 注意：TreeCache在初始化(调用start()方法)的时候会回调TreeCacheListener实例一个事TreeCacheEvent，
     * 而回调的TreeCacheEvent对象的Type为INITIALIZED，ChildData为null，此时event.getData().getPath()
     * 很有可能导致空指针异常，这里应该主动处理并避免这种情况。
     */

    @Test
    public void TreeCache() throws Exception {
        String treeCacheStr = "/cache/treeCache";

        curatorFramework.delete().deletingChildrenIfNeeded().forPath(treeCacheStr);
        System.out.println("删除节点");
        //创建treeCache
        TreeCache treeCache = new TreeCache(curatorFramework, treeCacheStr);

        TreeCacheListener treeCacheListener = (client, event) -> {
            System.out.println("事件类型：" + event.getType());
            if (event.getData() != null) {
                System.out.println("节点路径：" + event.getData().getPath() + "----节点数据：" + new String(event.getData().getData()));
            }
        };
        //加入监听
        treeCache.getListenable().addListener(treeCacheListener);
        //开始监听
        treeCache.start();
        String s1 = curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(treeCacheStr + "/a", "1".getBytes());
        System.out.println("创建节点" + s1);
        String s2 = curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(treeCacheStr + "/a/b", "1".getBytes());
        System.out.println("创建节点" + s2);

        Stat stat = curatorFramework.setData().forPath(treeCacheStr + "/a/b", "112".getBytes());
        System.out.println("节点状态：" + stat);
        //阻塞。等待输入
        System.in.read();
    }
}

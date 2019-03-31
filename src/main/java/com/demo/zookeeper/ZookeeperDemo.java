package com.demo.zookeeper;

import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.leader.*;
import org.apache.curator.framework.recipes.locks.*;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.lang.annotation.Repeatable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ZookeeperDemo
 * <p>
 * liwenbin
 * 2019/3/31 9:44
 */
public class ZookeeperDemo {
    private final static String zookeeperAddress = "192.168.1.232:2181";

    private final static String lock = "/lock/distributed_lock";
    private final static String leader = "/zktest/leader";
    private final static String LeaderLatch = "/zktest/LeaderLatch";

    byte[] bytes = {1, 2, 3};

    CuratorFramework curatorFramework = null;

    /**
     * 创建一个会话
     */
    public ZookeeperDemo() {
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
     * 创建临时有序节点
     *
     * @throws Exception
     */
    @Test
    public void createEpNode() throws Exception {
        String forPath = curatorFramework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/one", bytes);
        System.out.println(forPath);
    }


    /**
     * 持久有序节点
     *
     * @throws Exception
     */
    @Test
    public void cteateParentEqnode() throws Exception {
        String s = curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/two/b/c", bytes);
        System.out.println(s);
    }

    /**
     * 递归删除节点
     *
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        Void aVoid = curatorFramework.delete().deletingChildrenIfNeeded().forPath("/two");
    }

    //获取节点信息，并获取节点状态
    @Test
    public void getDate() throws Exception {
        byte[] bytes = curatorFramework.getData().storingStatIn(new Stat()).forPath("/two/a");
        System.out.println(bytes.toString());
    }

    //获取节点下所有子节点的路径
    @Test
    public void getChildren() throws Exception {
        List<String> strings = curatorFramework.getChildren().forPath("/two");
        System.out.println(strings);
    }

    //inTransaction事物操作

    /**
     * CuratorFramework的实例包含inTransaction( )接口方法，调用此方法开启一个ZooKeeper事务.
     * 可以复合create, setData, check, and/or delete 等操作然后调用commit()作为一个原子操作提交。
     */
    @Test
    public void TransationTest() throws Exception {
        Collection<CuratorTransactionResult> commit =
                curatorFramework.inTransaction()
                        .check()
                        .forPath("/two")
                        .and()
                        .create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath("/two/d")
                        .and()
                        .setData()
                        .forPath("/two/a", "1234".getBytes())
                        .and().commit();
        for (CuratorTransactionResult curatorTransactionResult : commit) {
            System.out.println("事物操作---" + curatorTransactionResult.toString());
        }
    }

    /**
     * 异步接口
     * <p>
     * 上面提到的创建、删除、更新、读取等方法都是同步的，Curator提供异步接口，引入了BackgroundCallback接口用于处理异步接口调用之后服务端返回的结果信息。
     * BackgroundCallback接口中一个重要的回调值为CuratorEvent，里面包含事件类型、响应吗和节点的详细信息。
     */
    @Test
    public void BackgroundCallback() throws Exception {
        Executor executor = Executors.newFixedThreadPool(5);

        String path = curatorFramework
                .create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .inBackground((curatorFramework, curatorEvent) -> {
                    System.out.println(String.format("eventType类型：%s,resultCode=%s", curatorEvent.getType(), curatorEvent.getResultCode()));
                }, executor)
                .forPath("/two/h");
        System.out.println(path);
        System.in.read();
    }

    /**
     * Path Cache
     * Path Cache用来监控一个ZNode的子节点. 当一个子节点增加， 更新，删除时， Path Cache会改变它的状态，
     * 会包含最新的子节点， 子节点的数据和状态，而状态的更变将通过PathChildrenCacheListener通知。
     * <p>
     * <p>
     * 只能监听子节点信息，不能监听子节点下的节点的更新变化！！！！！！！
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

    /**
     * Leader选举
     * <p>
     * 在分布式计算中， leader elections是很重要的一个功能， 这个选举过程是这样子的： 指派一个进程作为组织者，将任务分发给各节点。
     * 在任务开始前， 哪个节点都不知道谁是leader(领导者)或者coordinator(协调者). 当选举算法开始执行后， 每个节点最终会得到一个唯一的节点作为任务leader.
     * 除此之外， 选举还经常会发生在leader意外宕机的情况下，新的leader要被选举出来。
     * <p>
     * 在zookeeper集群中，leader负责写操作，然后通过Zab协议实现follower的同步，leader或者follower都可以处理读操作。
     * <p>
     * Curator 有两种leader选举的recipe,分别是LeaderSelector和LeaderLatch。
     * <p>
     * LeaderSelector是所有存活的客户端不间断的轮流做Leader，大同社会。
     * LeaderLatch是一旦选举出Leader，除非有客户端挂掉重新触发选举，否则不会交出领导权。
     * <p>
     * <p>
     * 首先我们创建了10个LeaderLatch，启动后它们中的一个会被选举为leader。 因为选举会花费一些时间，start后并不能马上就得到leader。
     * 通过hasLeadership查看自己是否是leader， 如果是的话返回true。
     * 可以通过.getLeader().getId()可以得到当前的leader的ID。
     * 只能通过close释放当前的领导权。
     * await是一个阻塞方法， 尝试获取leader地位，但是未必能上位。
     */

    @Test
    public void LeaderLatch() {
        String LeaderLatchStr = "/leader/leaderLatch";

        //客户端集合
        List<CuratorFramework> clients = Lists.newArrayList();
        //leaderlatche集合
        List<org.apache.curator.framework.recipes.leader.LeaderLatch> leaderLatches = Lists.newArrayList();
        try {
            for (int i = 0; i < 10; i++) {
                //创建客户端放入集合
                RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
                CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
                clients.add(client);

                //创建leaderlatch
                org.apache.curator.framework.recipes.leader.LeaderLatch latch = new LeaderLatch(client, LeaderLatchStr, "Client #" + i);
                //开始进行leader选取
                latch.addListener(new LeaderLatchListener() {
                    @Override
                    public void isLeader() {
                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy年mm月dd日hh时MM分ss秒");
                        System.out.println(dateFormat.format(new Date()) + "---我是Leader");
                    }

                    @Override
                    public void notLeader() {
                        System.out.println("我不是Leader。。。。。");
                    }
                });
                leaderLatches.add(latch);
                //启动客户端
                client.start();
                //启动latch
                latch.start();
            }
            System.out.println("线程休息10秒");
            Thread.sleep(10000);

            //现在的leader
            LeaderLatch leaderLatch = null;
            //寻找leader
            for (org.apache.curator.framework.recipes.leader.LeaderLatch latch : leaderLatches) {
                if (latch.hasLeadership()) {  ///返回true说明当前实例是leader
                    leaderLatch = latch;
                    System.out.println("找到了leader..,leader的id是" + latch.getId());
                    break;
                }
            }
            System.out.println("现在的leder是：" + leaderLatch.getId());
            System.out.println("释放的leder是：" + leaderLatch.getId());
            leaderLatch.close();
            System.out.println("线程休息10秒");
            Thread.sleep(10000);
            //选举leader
            for (org.apache.curator.framework.recipes.leader.LeaderLatch latch : leaderLatches) {
                if (latch.hasLeadership()) {  ///返回true说明当前实例是leader
                    leaderLatch = latch;
                    System.out.println("找到了leader..,leader的id是" + latch.getId());
                    break;
                }
            }
            System.out.println("现在的leder是：" + leaderLatch.getId());
            System.out.println("释放的leder是：" + leaderLatch.getId());
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            for (org.apache.curator.framework.recipes.leader.LeaderLatch leaderLatch : leaderLatches) {
                if (leaderLatch.getState() != null) {
                    CloseableUtils.closeQuietly(leaderLatch);
                }
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
        }
    }

    /**
     * LeaderSelector
     * LeaderSelector使用的时候主要涉及下面几个类：
     * <p>
     * LeaderSelector
     * LeaderSelectorListener
     * LeaderSelectorListenerAdapter
     * CancelLeadershipException
     */

    @Test
    public void LeaderSelector() {

        String leaderSelectorStr = "/leader/leaderSelector";
        //客户端集合
        List<CuratorFramework> clients = Lists.newArrayList();
        //leaderlatche集合
        List<LeaderSelector> leaderSelectorList = Lists.newArrayList();
        try {
            for (int i = 0; i < 10; i++) {
                //创建客户端放入集合
                RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
                CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
                clients.add(client);

                //创建LeaderSelector
                LeaderSelector leaderSelector = new LeaderSelector(client, leaderSelectorStr, new LeaderSelectorListener() {
                    @Override
                    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                        byte[] bytes = curatorFramework.getData().forPath(leaderSelectorStr);
                        System.out.println("takeLeadership----" + new String(bytes));
                    }

                    @Override
                    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                        try {
                            byte[] bytes = new byte[0];
                            bytes = curatorFramework.getData().forPath(leaderSelectorStr);
                            System.out.println("takeLeadership----" + new String(bytes));
                            System.out.println("connectionState----" + connectionState.name());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
//                leaderSelector.autoRequeue();
                leaderSelector.setId("leaderSelector_#" + i);
                leaderSelectorList.add(leaderSelector);
                //启动客户端
                client.start();
                //启动latch
                leaderSelector.start();
            }

            for (LeaderSelector exampleClient : leaderSelectorList) {
                String id = exampleClient.getId();
                System.out.println("遍历leaderSelector:" + id);
            }
            //等待输入
            System.in.read();
        } catch (Exception e) {

        } finally {
            System.out.println("关闭...");
            for (LeaderSelector exampleClient : leaderSelectorList) {
                CloseableUtils.closeQuietly(exampleClient);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
        }

    }


    /**
     * 共享锁操作
     * <p>
     * 普通锁，节点下面会产生一个节点
     */
    @Test
    public void shareLock() throws InterruptedException {
        String shareLockStr = "/lock/shareLock";
        ExecutorService service = Executors.newFixedThreadPool(5);
        service.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
                    CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
                    client.start();
                    InterProcessMutex mutex = new InterProcessMutex(client, shareLockStr);
                    //判断是否获取锁
                    try {
                        boolean acquire = mutex.acquire(5000, TimeUnit.SECONDS);
                        if (acquire) {
                            List<String> strings = client.getChildren().forPath(shareLockStr);
                            byte[] bytes = client.getData().forPath(shareLockStr + "/" + strings.get(0));
                            System.out.println("获取到锁啦。。。" + strings + "-----数据为" + new String(bytes));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            System.out.println("释放锁啦。。。");
                            mutex.release();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }
            }
        });
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }

    /**
     * 共享锁操作
     * 这个锁和上面的InterProcessMutex相比，就是少了Reentrant的功能，也就意味着它不能在同一个线程中重入。
     * 这个类是InterProcessSemaphoreMutex,使用方法和InterProcessMutex类似
     * <p>
     * <p>
     * 运行后发现，有且只有一个client成功获取第一个锁(第一个acquire()方法返回true)，然后它自己阻塞在第二个acquire()方法，获取第二个锁超时；其他所有的客户端都阻塞在第一个acquire()方法超时并且抛出异常。
     * <p>
     * 这样也就验证了InterProcessSemaphoreMutex实现的锁是不可重入的。
     * <p>
     * 下面会产生  leases ,locks
     */
    @Test
    public void sharedLock() throws InterruptedException {
        String shareLockStr = "/lock/shareLock";
        ExecutorService service = Executors.newFixedThreadPool(5);
        service.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
                    CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
                    client.start();
                    InterProcessSemaphoreMutex mutex = new InterProcessSemaphoreMutex(client, shareLockStr);
                    //判断是否获取锁
                    try {
                        boolean acquire = mutex.acquire(5000, TimeUnit.SECONDS);
                        if (!acquire) {
                            throw new IllegalStateException(" 不能得到互斥锁");
                        } else {
                            System.out.println(" 已获取到互斥锁");
                            List<String> strings = client.getChildren().forPath(shareLockStr);
                            byte[] bytes = client.getData().forPath(shareLockStr + "/" + strings.get(1));
                            System.out.println("获取到锁啦。。。" + strings + "-----数据为" + new String(bytes));
                        }
                        boolean acquire2 = mutex.acquire(5000, TimeUnit.SECONDS);
                        if (!acquire2) {
                            throw new IllegalStateException(" 不能得到互斥锁");
                        } else {
                            System.out.println(" 再次获取到互斥锁");
                            List<String> strings = client.getChildren().forPath(shareLockStr);
                            byte[] bytes = client.getData().forPath(shareLockStr + "/" + strings.get(1));
                            System.out.println("获取到锁啦。。。" + strings + "-----数据为" + new String(bytes));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            System.out.println("释放锁啦。。。");
                            mutex.release();
                            mutex.release(); // 获取锁几次 释放锁也要几次
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }
            }
        });
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }

    /**
     * 读锁和写锁
     * <p>
     * 可重入读写锁—Shared Reentrant Read Write Lock
     * 类似JDK的ReentrantReadWriteLock。一个读写锁管理一对相关的锁。一个负责读操作，另外一个负责写操作。读操作在写锁没被使用时可同时由多个进程使用，而写锁在使用时不允许读(阻塞)。
     * <p>
     * 此锁是可重入的。一个拥有写锁的线程可重入读锁，但是读锁却不能进入写锁。这也意味着写锁可以降级成读锁， 比如请求写锁 —>请求读锁—>释放读锁 —->释放写锁。从读锁升级成写锁是不行的。
     * <p>
     * 可重入读写锁主要由两个类实现：InterProcessReadWriteLock、InterProcessMutex。使用时首先创建一个InterProcessReadWriteLock实例，然后再根据你的需求得到读锁或者写锁，读写锁的类型是InterProcessMutex。
     *
     * @throws InterruptedException
     */
    @Test
    public void ReadWriteLock() throws InterruptedException {


        String shareLockStr = "/lock/shareLock";
        ExecutorService service = Executors.newFixedThreadPool(5);
        service.submit(new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < 10; i++) {
                    RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
                    CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
                    client.start();
                    InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, shareLockStr);

                    InterProcessMutex readLock = null;
                    InterProcessMutex writeLock = null;
                    //判断是否获取锁
                    try {
                        //一个拥有写锁的线程可重入读锁，但是读锁却不能进入写锁。这也意味着写锁可以降级成读锁， 比如请求写锁 —>请求读锁—>释放读锁 —->释放写锁。
                        //获取写锁
                        writeLock = lock.writeLock();
                        boolean acquire2 = writeLock.acquire(5000, TimeUnit.SECONDS);
                        if (!acquire2) {
                            throw new IllegalStateException(" 不能得到写锁");
                        } else {
                            List<String> strings = client.getChildren().forPath(shareLockStr);
                            byte[] bytes = client.getData().forPath(shareLockStr + "/" + strings.get(1));
                            System.out.println("获取写锁啦。。。" + strings + "-----数据为" + new String(bytes));
                        }
                        //获取读锁
                        readLock = lock.readLock();
                        boolean acquire = readLock.acquire(5000, TimeUnit.SECONDS);
                        if (!acquire) {
                            throw new IllegalStateException(" 不能得到读锁");
                        } else {
                            List<String> strings = client.getChildren().forPath(shareLockStr);
                            byte[] bytes = client.getData().forPath(shareLockStr + "/" + strings.get(1));
                            System.out.println("获取读锁啦。。。" + strings + "-----数据为" + new String(bytes));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            System.out.println("释放锁啦。。。");
                            readLock.release();
                            writeLock.release(); // 获取锁几次 释放锁也要几次
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }

    /**
     * 信号量—Shared Semaphore
     * <p>
     * 一个计数的信号量类似JDK的Semaphore。 JDK中Semaphore维护的一组许可(permits)，而Curator中称之为租约(Lease)。 有两种方式可以决定semaphore的最大租约数。
     * 第一种方式是用户给定path并且指定最大LeaseSize。第二种方式用户给定path并且使用SharedCountReader类。如果不使用SharedCountReader, 必须保证所有实例
     * 在多进程中使用相同的(最大)租约数量,否则有可能出现A进程中的实例持有最大租约数量为10，但是在B进程中持有的最大租约数量为20，此时租约的意义就失效了。
     * <p>
     * 这次调用acquire()会返回一个租约对象。 客户端必须在finally中close这些租约对象，否则这些租约会丢失掉。 但是， 但是，如果客户端session由于某种原因
     * 比如crash丢掉， 那么这些客户端持有的租约会自动close， 这样其它客户端可以继续使用这些租约。 租约还可以通过下面的方式返还：
     */
    @Test
    public void semaphore() throws Exception {
        String semaphoreStr = "/lock/semaphore";

        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
        client.start();
        //会生成十个临时节点
        InterProcessSemaphoreV2 v2 = new InterProcessSemaphoreV2(client, semaphoreStr, 10);
        Collection<Lease> acquire = v2.acquire(5);
        //获取组约数
        System.out.println("获取组约数：" + acquire.size());

        Lease lease = v2.acquire();
        System.out.println("获得另一个租约");

        //########################start########################
        /**
         *读取资源操作
         *
         * resource.use();
         */
        //########################end########################

        //一共10个租约，已经获取了6个，再获取5个，就超出了范围，素有会返回null.
        Collection<Lease> acquire2 = v2.acquire(5, 10, TimeUnit.SECONDS);
        System.out.println("Should timeout and acquire return " + acquire2);

        //返回一个就是删除一个节点
        System.out.println("返回一个lease");
        v2.returnLease(lease);
        System.out.println("返回另外五个leases");
        v2.returnAll(acquire);


        /**
         *
         * 首先我们先获得了5个租约， 最后我们把它还给了semaphore。 接着请求了一个租约，因为semaphore还有5个租约，
         * 所以请求可以满足，返回一个租约，还剩4个租约。 然后再请求一个租约，因为租约不够，阻塞到超时，还是没能满足，
         * 返回结果为null(租约不足会阻塞到超时，然后返回null，不会主动抛出异常；如果不设置超时时间，会一致阻塞)。
         *
         * 上面说讲的锁都是公平锁(fair)。 总ZooKeeper的角度看， 每个客户端都按照请求的顺序获得锁，不存在非公平的抢占的情况。
         */
    }


    /**
     * 多共享锁对象 —Multi Shared Lock
     * Multi Shared Lock是一个锁的容器。 当调用acquire()， 所有的锁都会被acquire()，如果请求失败，所有的锁都会被release。 同样调用release时所有的锁都被release(失败被忽略)。 基本上，它就是组锁的代表，在它上面的请求释放操作都会传递给它包含的所有的锁。
     * <p>
     * 主要涉及两个类：
     * <p>
     * InterProcessMultiLock
     * InterProcessLock
     * 它的构造函数需要包含的锁的集合，或者一组ZooKeeper的path。
     */
    @Test
    public void multiLock() throws Exception {
        String multiLockStr = "/lock/multiLockOne";
        String multiLockStr2 = "/lock/multiLockTwo";

        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
        client.start();

        InterProcessLock lock1 = new InterProcessMutex(client, multiLockStr);
        InterProcessLock lock2 = new InterProcessSemaphoreMutex(client, multiLockStr2);

        InterProcessMultiLock lock = new InterProcessMultiLock(Arrays.asList(lock1, lock2));

        boolean acquire = lock.acquire(10, TimeUnit.SECONDS);
        if (!acquire) {
            System.out.println("无法获取锁");
        }

        System.out.println("已经获取锁");
        System.out.println("已经获取锁，lock1" + lock1.isAcquiredInThisProcess());
        System.out.println("已经获取锁，lock2" + lock2.isAcquiredInThisProcess());

        //独占访问资源操作  -----------------------省略

        System.out.println("释放锁");
        lock.release();

        System.out.println("已经获取锁，lock1" + lock1.isAcquiredInThisProcess());
        System.out.println("已经获取锁，lock2" + lock2.isAcquiredInThisProcess());
    }

    /**
     * 分布式计数器
     * 顾名思义，计数器是用来计数的, 利用ZooKeeper可以实现一个集群共享的计数器。 只要使用相同的
     * path就可以得到最新的计数器值， 这是由ZooKeeper的一致性保证的。
     * Curator有两个计数器， 一个是用int来计数(SharedCount)，一个用long来计数(DistributedAtomicLong)
     * <p>
     * <p>
     * 分布式int计数器—SharedCount
     * 这个类使用int类型来计数。 主要涉及三个类。
     * <p>
     * SharedCount
     * SharedCountReader
     * SharedCountListener
     * SharedCount代表计数器， 可以为它增加一个SharedCountListener，当计数器改变时此Listener可以监听到改变的事件，
     * 而SharedCountReader可以读取到最新的值， 包括字面值和带版本信息的值VersionedValue。
     */

    public class SharedCounter implements SharedCountListener {

        String SharedCounterStr = "/lock/multiLockOne";

        @Test
        public void getshareCounter() {
            RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, retry);
            client.start();
            try {
                final Random rand = new Random();
                SharedCounter sharedCounter = new SharedCounter();

                SharedCount counter = new SharedCount(client, SharedCounterStr, 0);

                counter.addListener(sharedCounter);
                counter.start();

                List<SharedCount> arrayList = Lists.newArrayList();
                ExecutorService executorService = Executors.newFixedThreadPool(5);


                for (int i = 0; i < 10; i++) {
                    final SharedCount sharedCount = new SharedCount(client, SharedCounterStr, 0);
                    arrayList.add(sharedCount);

                    executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                sharedCount.start();
                                System.out.println("线程休眠");
                                Thread.sleep(rand.nextInt(10000));

                                System.out.println(sharedCount.trySetCount(sharedCount.getVersionedValue(), sharedCount.getCount() + rand.nextInt(10)));
                            } catch (Exception e) {
                            }

                        }
                    });
                    executorService.shutdown();
                    executorService.awaitTermination(10, TimeUnit.MINUTES);

                    for (int j = 0; j < 10; j++) {
                        arrayList.get(j).close();

                    }
                    sharedCount.close();
                }
                Thread.sleep(Integer.MAX_VALUE);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void countHasChanged(SharedCountReader sharedCountReader, int i) throws Exception {
            System.out.println("Counter's value is changed to " + i);
        }

        @Override
        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
            System.out.println("状态改变" + connectionState.toString());
        }
    }

}

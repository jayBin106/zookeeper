package com.demo.zookeeper;

import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
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

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * ZookeeperDemo
 * <p>
 * liwenbin
 * 2019/3/31 9:44
 */
public class ZookeeperDemo {
    public final static String zookeeperAddress = "192.168.1.232:2181";
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
                curatorFramework
                        .inTransaction()
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

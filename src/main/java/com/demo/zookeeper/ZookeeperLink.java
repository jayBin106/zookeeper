package com.demo.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ZookeeperLink
 * <p>
 * liwenbin
 * 2019/3/24 14:05
 */
public class ZookeeperLink {
    private final static String str = "192.168.1.232:2181";
    private final static String lock = "/lock/distributed_lock";
    private final static String leader = "/zktest/leader";
    private final static String LeaderLatch = "/zktest/LeaderLatch";

    CuratorFramework client = null;

    public void getClinet() {
        //重连策略
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(3000, 3);
        client = CuratorFrameworkFactory.newClient(str, retry);
        client.start();
    }

    /**
     * 创建一个永久有序节点
     *
     * @throws Exception
     */
    @Test
    public void cteateNode() throws Exception {
        byte[] bytes = {1, 2, 3};
        String mic = "/mic";
        String forPath = client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(mic, bytes);
        System.out.println(forPath);
        byte[] bytes1 = client.getData().forPath(forPath);
        System.out.println(bytes1);
    }


    /**
     * 测试分布式锁
     */


    @Test
    public void test() {
        getClinet();

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 5; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            dowork();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }

                    }
                }
            });
        }
    }


    /**
     * 线程争抢锁
     *
     * @throws Exception
     */
    @Test
    public void dowork() throws Exception {
        InterProcessMutex mutex = new InterProcessMutex(client, lock);
        try {
            //获得锁
            mutex.acquire();
            System.out.println("线程id：" + Thread.currentThread().getId() + "----->获得锁");
            Thread.sleep(1000);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            System.out.println("线程id：" + Thread.currentThread().getId() + "----->释放锁");
            mutex.release();
        }
    }


    /**
     * Leader选举
     * <p>
     * 选举可以用来实现Master-Slave模式，也可以用来实现主备切换等功能。Curator提供两种方式实现选举：
     * LeaderSelector 和 LeaderLatch。两种方法都可以使用，LeaderLatch语法较为简单一点，LeaderSelector控制度更高一些。
     * <p>
     * <p>
     * LeaderSelector的内部使用分布式锁InterProcessMutex实现， 并且在LeaderSelector中添加一个Listener，
     * 当获取到锁的时候执行回调函数takeLeadership。函数执行完成之后就调用InterProcessMutex.release()释放锁，也就是放弃Leader的角色。
     */
    @Test
    public void leaderGet() throws IOException {
        getClinet();

        LeaderSelectorListenerAdapter listenerAdapter = new LeaderSelectorListenerAdapter() {
            @Override
            public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                System.out.println("一马当先。。");
                Thread.sleep(1000);
                System.out.println("弃马投降。。");
            }
        };
        LeaderSelector selector = new LeaderSelector(client, leader, listenerAdapter);
        selector.autoRequeue();
        selector.start();
        //等待输入阻塞。。
        System.in.read();
    }


    /**
     * 同样是实现Leader选举的LeaderLatch并没有通过InterProcessMutex实现，它使用了原生的创建EPHEMERAL_SEQUENTIAL节点的功能再次实现了一遍。
     * 同样的在isLeader方法中需要实现Leader的业务需求，但是一旦isLeader方法返回，就相当于Leader角色放弃了，重新进入选举过程。
     */
    @Test
    public void getLeaderLatch() throws IOException {
        getClinet();

        LeaderLatch leaderLatch = new LeaderLatch(client, LeaderLatch);
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.println("一马当先。。");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("弃马投降。。");
            }

            @Override
            public void notLeader() {
                System.out.println("我不是一个leader..");

            }
        });
        try {
            leaderLatch.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        //等待输入阻塞。。
        System.in.read();

    }
}

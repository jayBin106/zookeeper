package com.demo.zookeeper.分布式计数器;

import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private final static String zookeeperAddress = "192.168.1.232:2181";

    String SharedCounterStr = "/lock/SharedCounter";

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
            }
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);

            for (int j = 0; j < 10; j++) {
                arrayList.get(j).close();
            }
            counter.close();
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
package com.demo.zookeeper.分布式屏障;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.demo.zookeeper.ZookeeperDemo.zookeeperAddress;

/**
 * 分布式屏障—Barrier
 * 分布式Barrier是这样一个类： 它会阻塞所有节点上的等待进程，直到某一个被满足， 然后所有的节点继续进行。
 * <p>
 * 比如赛马比赛中， 等赛马陆续来到起跑线前。 一声令下，所有的赛马都飞奔而出。
 * <p>
 * DistributedBarrier
 */
public class DistributedBarrierDemo {

    private static final int QTY = 5;
    private static final String PATH = "/examples/barrier";

    public static void main(String[] args) throws Exception {
        //初始话客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(1000, 3));
        client.start();
        //设置多个线程
        ExecutorService service = Executors.newFixedThreadPool(QTY);
        //DistributedBarrier类实现了栅栏的功能。
        DistributedBarrier controlBarrier = new DistributedBarrier(client, PATH);
        //首先你需要设置栅栏，它将阻塞在它上面等待的线程:
        controlBarrier.setBarrier();
        for (int i = 0; i < QTY; ++i) {
            final DistributedBarrier barrier = new DistributedBarrier(client, PATH);
            final int index = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep((long) (3 * Math.random()));
                        System.out.println("Client #" + index + " waits on Barrier");
                        //需要阻塞的线程调用方法等待放行条件:
                        barrier.waitOnBarrier();
                        System.out.println("Client #" + index + " begins");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        Thread.sleep(10000);
        System.out.println("all Barrier instances should wait the condition");
        //移除栅栏，所有等待的线程将继续执行
        controlBarrier.removeBarrier();
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
        Thread.sleep(20000);
    }
}
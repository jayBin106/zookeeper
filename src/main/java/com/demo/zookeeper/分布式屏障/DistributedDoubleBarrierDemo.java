package com.demo.zookeeper.分布式屏障;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.demo.zookeeper.zookeeper基本操作.ZookeeperDemo.zookeeperAddress;

/**
 * 双栅栏—DistributedDoubleBarrier
 * 双栅栏允许客户端在计算的开始和结束时同步。当足够的进程加入到双栅栏时，
 * 进程开始计算， 当计算完成时，离开栅栏。 双栅栏类是DistributedDoubleBarrier。 构造函数为:
 * <p>
 * <p>
 * 就像百米赛跑比赛， 发令枪响， 所有的运动员开始跑，等所有的运动员跑过终点线，比赛才结束。
 */
public class DistributedDoubleBarrierDemo {

    private static final int QTY = 5;
    private static final String PATH = "/examples/barrier";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(1000, 3));
        client.start();
        //设置线程
        ExecutorService service = Executors.newFixedThreadPool(QTY);
        for (int i = 0; i < QTY; ++i) {
            //设置双栅栏
            final DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, PATH, QTY);
            final int index = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep((long) (3 * Math.random()));
                        System.out.println("Client #" + index + " enters");
                        //，当enter()方法被调用时，成员被阻塞，直到所有的成员都调用了enter()
                        barrier.enter();
                        System.out.println("Client #" + index + " begins");
                        Thread.sleep((long) (3000 * Math.random()));
                        //当leave()方法被调用时，它也阻塞调用线程，直到所有的成员都调用了leave()
                        barrier.leave();
                        System.out.println("Client #" + index + " leave");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
        Thread.sleep(Integer.MAX_VALUE);
    }
}
package com.demo.zookeeper.分布式锁;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.demo.zookeeper.ZookeeperDemo.zookeeperAddress;

/**
 * lock
 * <p>
 * liwenbin
 * 2019/4/1 13:39
 * 提醒：
 * <p>
 * 1.推荐使用ConnectionStateListener监控连接的状态，因为当连接LOST时你不再拥有锁
 * <p>
 * 2.分布式的锁全局同步， 这意味着任何一个时间点不会有两个客户端都拥有相同的锁。
 */
public class lock {
    /**
     * 共享锁操作
     * <p>
     * 可重入共享锁—Shared Reentrant Lock
     * Shared意味着锁是全局可见的， 客户端都可以请求锁。 Reentrant和JDK的ReentrantLock类似，
     * 即可重入， 意味着同一个客户端在拥有锁的同时，可以多次获取，不会被阻塞。 它是由类InterProcessMutex来实现。 它的构造函数为：
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
     * 多共享锁对象 —Multi Shared Lock
     * Multi Shared Lock是一个锁的容器。 当调用acquire()， 所有的锁都会被acquire()，如果请求失败，所有的锁都会被release。
     * 同样调用release时所有的锁都被release(失败被忽略)。 基本上，它就是组锁的代表，在它上面的请求释放操作都会传递给它包含的所有的锁。
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
}

package com.demo.zookeeper.Leader选举;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 当实例被选为leader之后，
 * 调用takeLeadership()方法进行业务逻辑处理，处理完成即释放领导权。
 * <p>
 * 其中autoRequeue()方法的调用确保此实例在释放领导权后还可能获得领导权。
 */

public class LeaderSelectorAdapter extends LeaderSelectorListenerAdapter implements Closeable {
    private final String name;
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    public LeaderSelectorAdapter(CuratorFramework client, String path, String name) {
        this.name = name;
        leaderSelector = new LeaderSelector(client, path, this);
        //autoRequeue()方法的调用确保此实例在释放领导权后还可能获得领导权。
//        leaderSelector.autoRequeue();
    }

    public void start() throws IOException {
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    /**
     * 在方法中获取领导权，执行完逻辑后释放领导权
     *
     * @param client
     * @throws Exception
     */
    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        final int waitSeconds = (int) (5 * Math.random()) + 1;
        System.out.println(name + " 不是leader. 等待 " + waitSeconds + " 秒...");
        System.out.println(name + " 已经是leader " + leaderCount.getAndIncrement() + " time(s) before.");
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            System.err.println(name + " 被中断");
            Thread.currentThread().interrupt();
        } finally {
            System.out.println(name + " 放弃领导权");
        }
    }
}
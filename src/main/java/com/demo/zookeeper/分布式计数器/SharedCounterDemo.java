package com.demo.zookeeper.分布式计数器;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.demo.zookeeper.ZookeeperDemo.zookeeperAddress;

/**
 * 分布式int计数器—SharedCount
 * 这个类使用int类型来计数。 主要涉及三个类。
 * <p>
 * SharedCount
 * SharedCountReader
 * SharedCountListener
 * SharedCount代表计数器， 可以为它增加一个SharedCountListener，
 * 当计数器改变时此Listener可以监听到改变的事件，而SharedCountReader可以读取到最新的值， 包括字面值和带版本信息的值VersionedValue。
 *
 *
 * 在这个例子中，我们使用baseCount来监听计数值(addListener方法来添加SharedCountListener )。 任意的SharedCount， 只要使用相同的path，都可以得到这个计数值。
 * 然后我们使用5个线程为计数值增加一个10以内的随机数。相同的path的SharedCount对计数值进行更改，将会回调给baseCount的SharedCountListener。
 */
public class SharedCounterDemo implements SharedCountListener {

    private static final int QTY = 5;
    private static final String PATH = "/examples/counter";

    public static void main(String[] args) throws IOException, Exception {
        final Random rand = new Random();
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(1000, 3));
        client.start();

        //创建计数器
        SharedCount baseCount = new SharedCount(client, PATH, 0);
        SharedCounterDemo example = new SharedCounterDemo();
        //加入SharedCountListener监听
        baseCount.addListener(example);
        //启动计数器
        baseCount.start();

        List<SharedCount> examples = Lists.newArrayList();
        ExecutorService service = Executors.newFixedThreadPool(QTY);
        for (int i = 0; i < QTY; ++i) {
            //创建计数器
            final SharedCount count = new SharedCount(client, PATH, 0);
            //加入集合
            examples.add(count);
            count.start();
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(rand.nextInt(10000));
                        VersionedValue<Integer> versionedValue = count.getVersionedValue();
                        int countCount = count.getCount();
                        /**
                         * 这里我们使用trySetCount去设置计数器。 第一个参数提供当前的VersionedValue,如果期间其它client更新了此计数值，
                         * 你的更新可能不成功， 但是这时你的client更新了最新的值，所以失败了你可以尝试再更新一次。 而setCount是强制更新计数器的值。
                         *
                         * 注意计数器必须start,使用完之后必须调用close关闭它。
                         * 强烈推荐使用ConnectionStateListener。 在本例中SharedCountListener扩展ConnectionStateListener。
                         */
                        boolean setCount = count.trySetCount(versionedValue, countCount + 1);
                        System.out.println("versionValue：" + versionedValue);
                        System.out.println("countCount：" + countCount);
                        System.out.println("是否设置成功:" + setCount);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);

        for (int i = 0; i < QTY; ++i) {
            examples.get(i).close();
        }
        baseCount.close();
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Override
    public void stateChanged(CuratorFramework arg0, ConnectionState arg1) {
        System.out.println("状态变为: " + arg1.toString());
    }

    @Override
    public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
        System.out.println("值变为： " + newCount);
    }
}
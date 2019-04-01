package com.demo.zookeeper.分布式队列;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.queue.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * DistributedQueueDemoMe
 * <p>
 * liwenbin
 * 2019/3/31 18:22
 * <p>
 * <p>
 * 分布式队列
 * 使用Curator也可以简化Ephemeral Node (临时节点)的操作。Curator也提供ZK Recipe的分布式队列实现。 利用ZK的 PERSISTENTS_EQUENTIAL节点，
 * 可以保证放入到队列中的项目是按照顺序排队的。 如果单一的消费者从队列中取数据， 那么它是先入先出的，这也是队列的特点。 如果你严格要求顺序，
 * 你就的使用单一的消费者，可以使用Leader选举只让Leader作为唯一的消费者。
 * <p>
 * 但是， 根据Netflix的Curator作者所说， ZooKeeper真心不适合做Queue，或者说ZK没有实现一个好的Queue，详细内容可以看 Tech Note 4， 原因有五：
 * <p>
 * ZK有1MB 的传输限制。 实践中ZNode必须相对较小，而队列包含成千上万的消息，非常的大。
 * 如果有很多节点，ZK启动时相当的慢。 而使用queue会导致好多ZNode. 你需要显著增大 initLimit 和 syncLimit.
 * ZNode很大的时候很难清理。Netflix不得不创建了一个专门的程序做这事。
 * 当很大量的包含成千上万的子节点的ZNode时， ZK的性能变得不好
 * ZK的数据库完全放在内存中。 大量的Queue意味着会占用很多的内存空间。
 * 尽管如此， Curator还是创建了各种Queue的实现。 如果Queue的数据量不太多，数据量不太大的情况下，酌情考虑，还是可以使用的。
 */
public class DistributedQueueDemoMe {
    private final static String zookeeperAddress = "192.168.1.232:2181";
    private static final String PATH = "/duiLie/queue";

    /**
     * 分布式队列—DistributedQueue
     * DistributedQueue是最普通的一种队列。 它设计以下四个类：
     * <p>
     * QueueBuilder - 创建队列使用QueueBuilder,它也是其它队列的创建类
     * QueueConsumer - 队列中的消息消费者接口
     * QueueSerializer - 队列消息序列化和反序列化接口，提供了对队列中的对象的序列化和反序列化
     * DistributedQueue - 队列实现类
     * QueueConsumer是消费者，它可以接收队列的数据。处理队列中的数据的代码逻辑可以放在QueueConsumer.consumeMessage()中。
     * <p>
     * 正常情况下先将消息从队列中移除，再交给消费者消费。但这是两个步骤，不是原子的。可以调用Builder的lockPath()消费者加锁，
     * 当消费者消费数据时持有锁，这样其它消费者不能消费此消息。如果消费失败或者进程死掉，消息可以交给其它进程。这会带来一点性能的损失。
     * 最好还是单消费者模式使用队列。
     */
    public static void main(String[] args) throws Exception {
        CuratorFramework clientA = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, new ExponentialBackoffRetry(1000, 3));
        clientA.start();
        CuratorFramework clientB = CuratorFrameworkFactory.newClient(zookeeperAddress, 5000, 3000, new ExponentialBackoffRetry(1000, 3));
        clientB.start();

        //QueueBuilder  创建队列使用QueueBuilder,它也是其它队列的创建类
        //QueueConsumer - 队列中的消息消费者接口
        //QueueSerializer - 队列消息序列化和反序列化接口，提供了对队列中的对象的序列化和反序列化

        //DistributedQueue - 队列实现类
        DistributedQueue<String> idQueueA;
        QueueBuilder<String> builderA = QueueBuilder.builder(clientA, createQueueConsumer("A"), createQueueSerializer(), PATH);
        idQueueA = builderA.buildQueue();
        idQueueA.start();

        DistributedQueue<String> idQueueB;
        QueueBuilder<String> builderB = QueueBuilder.builder(clientB, createQueueConsumer("B"), createQueueSerializer(), PATH);
        idQueueB = builderB.buildQueue();
        idQueueB.start();

        for (int i = 0; i < 100; i++) {
            System.out.println(i);
            idQueueA.put("A--" + i);
            Thread.sleep(1000);
            idQueueB.put("B--" + i);
        }
        System.in.read();

        idQueueB.close();
        idQueueA.close();
        clientB.close();
        clientA.close();

        System.out.println("game over....");

    }

    /**
     * 队列消息序列化实现类
     */
    private static QueueSerializer<String> createQueueSerializer() {
        return new QueueSerializer<String>() {
            @Override
            public byte[] serialize(String s) {
                return new byte[0];
            }

            @Override
            public String deserialize(byte[] bytes) {
                return new String(bytes);
            }
        };
    }

    /**
     * 定义队列消费者
     */
    private static QueueConsumer<String> createQueueConsumer(final String name) {
        return new QueueConsumer<String>() {
            @Override
            public void consumeMessage(String s) throws Exception {
                System.out.println("消费消息(" + name + ")-----------" + s);
            }

            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                System.out.println("连接状态改变: " + connectionState.name());

            }
        };
    }


}

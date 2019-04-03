package com.demo.zookeeper.Leader选举;

import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static com.demo.zookeeper.zookeeper基本操作.ZookeeperDemo.zookeeperAddress;

/**
 * Leader
 * <p>
 * liwenbin
 * 2019/4/1 9:56
 */
public class Leader {
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
            Random random = new Random();
            System.out.println("线程休息,随机秒");
            Thread.sleep(random.nextInt(10000));

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

            System.out.println("线程休息,随机秒");
            Thread.sleep(random.nextInt(10000));
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
            //关闭客户端和leaderLatch
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

        Random random = new Random();

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
                        List<String> strings = curatorFramework.getChildren().forPath(leaderSelectorStr);
                        System.out.println("数组长度：" + strings.size());
                        for (String string : strings) {
                            Stat stat = new Stat();
                            byte[] bytes = curatorFramework.getData().storingStatIn(stat).forPath(leaderSelectorStr + "/" + string);
                            System.out.println("获取数据—" + new String(bytes));
                            System.out.println(stat.getCversion());
                        }
                        System.out.println("获取领导权—" + strings);
                        Thread.sleep(random.nextInt(5000));
                        System.out.println("放弃领导权！！");
                    }

                    @Override
                    public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                        try {
                            System.out.println("连接状态--" + connectionState.name());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                leaderSelector.autoRequeue();  //是否可以重新获取领导权
                leaderSelector.setId("leaderSelector_#" + i);
                leaderSelectorList.add(leaderSelector);
                //启动客户端
                client.start();
                //启动latch
                leaderSelector.start();
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

}

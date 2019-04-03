package com.demo.zookeeper.ACL权限;

import com.google.common.collect.Lists;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.springframework.util.Base64Utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static com.demo.zookeeper.zookeeper基本操作.ZookeeperDemo.zookeeperAddress;

/**
 * aclTest
 * <p>
 * liwenbin
 * 2019/4/3 9:57
 */
public class aclTest {
    static CuratorFramework curator = null;
    static CuratorFramework curatorT = null;

    //初始化客户端
    public aclTest() {
        AuthInfo authInfo = new AuthInfo("digest", "imooc1:123456".getBytes());
        List<AuthInfo> authInfos = Lists.newArrayList();
        authInfos.add(authInfo);
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);

        curator = CuratorFrameworkFactory
                .builder()
                .connectString(zookeeperAddress)
                .authorization(authInfos)
                .retryPolicy(retry)
                .build();
        curator.start();
    }


    //为节点创建权限
    @org.junit.Test
    public void Test() throws Exception {
        List<ACL> acls = new ArrayList<ACL>();
        Id imooc1 = new Id("digest", AclUtils.getDigestUserPwd("imooc1:123456"));
        Id imooc2 = new Id("digest", AclUtils.getDigestUserPwd("imooc2:123456"));
        acls.add(new ACL(ZooDefs.Perms.ALL, imooc1));
        acls.add(new ACL(ZooDefs.Perms.READ, imooc2));
        acls.add(new ACL(ZooDefs.Perms.DELETE | ZooDefs.Perms.CREATE, imooc2));

        Stat stat = curator.setACL().withACL(acls).forPath("/disconfig");
        System.out.println(stat.toString());
    }


    public static void aclTest3() {
        AuthInfo authInfo = new AuthInfo("digest", "imooc2:123456".getBytes());
        List<AuthInfo> authInfos = Lists.newArrayList();
        authInfos.add(authInfo);
        RetryPolicy retry = new ExponentialBackoffRetry(1000, 3);
        curatorT = CuratorFrameworkFactory
                .builder()
                .authorization(authInfos)
                .connectString(zookeeperAddress)
                .retryPolicy(retry)
                .build();
        curatorT.start();
    }

    //用拥有权限的用户去访问节点数据
    @Test
    public void TT() throws Exception {
        aclTest3();
        byte[] bytes = curatorT.getData().forPath("/disconfig");
        System.out.println(new String(bytes));
    }

    public static void main(String[] args) throws Exception {
        aclTest test = new aclTest();
        test.Test();
        test.TT();
    }

}

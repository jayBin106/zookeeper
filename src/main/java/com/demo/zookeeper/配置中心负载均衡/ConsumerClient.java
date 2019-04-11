package com.demo.zookeeper.配置中心负载均衡;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.Collection;
import java.util.Map;
import java.util.Random;

import static com.demo.zookeeper.zookeeper基本操作.ZookeeperDemo.zookeeperAddress;

import org.junit.Test;

/**
 * @author: wangsaichao
 * @date: 2018/9/30
 * @description: 加入依赖
 * <p>
 * <p>
 */
public class ConsumerClient {

    //服务集合
    static Map<String, String> serverMap = null;

    @Test
    public void testthrows() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(1000, 3));
        client.start();
        client.blockUntilConnected();

        ServiceDiscovery<InstanceDetails> serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                .client(client)
                .basePath(InstanceDetails.ROOT_PATH)
                .serializer(new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class))
                .build();
        serviceDiscovery.start();

        //监听子节点变化
        TreeCacheListener listener = (ccc, event) -> {
            if (event.getData() != null) {
                byte[] bytes = event.getData().getData();
                String path = event.getData().getPath();
                System.out.println("类型:" + event.getType());
                System.out.println("路径:" + path);
                System.out.println("数据:" + new String(bytes));
            }
        };
        TreeCache treeCache = new TreeCache(client, InstanceDetails.ROOT_PATH);
        treeCache.getListenable().addListener(listener);
        treeCache.start();


        boolean flag = true;
        //根据名称获取服务
        while (flag) {
            Collection<ServiceInstance<InstanceDetails>> services = serviceDiscovery.queryForInstances("OrderService");
            if (services.size() == 0) {
                System.out.println("当前没有发现服务");
                Thread.sleep(10 * 1000);
            }
            Object[] objects = services.toArray();
            //负载均衡规则，随机获取
            Random random = new Random();
            int nextInt = random.nextInt(objects.length);
            ServiceInstance serviceInstance = (ServiceInstance) objects[nextInt];
            String address = serviceInstance.getAddress();
            System.out.println(address);
//            for (ServiceInstance<InstanceDetails> service : services) {
//                //获取请求的scheme 例如：http://127.0.0.1:8080
//                String uriSpec = service.buildUriSpec();
//                //获取服务的其他信息
//                InstanceDetails payload = service.getPayload();
//                //服务描述
//                String serviceDesc = payload.getServiceDesc();
//                //获取该服务下的所有接口
//                Map<String, InstanceDetails.Service> allService = payload.getServices();
//                Set<Map.Entry<String, InstanceDetails.Service>> entries = allService.entrySet();
//
//                for (Map.Entry<String, InstanceDetails.Service> entry : entries) {
//                    System.out.println(serviceDesc + uriSpec
//                            + "/" + service.getName()
//                            + "/" + entry.getKey()
//                            + " 该方法需要的参数为："
//                            + entry.getValue().getParams().toString());
//                }
//            }
            Thread.sleep(2000);
        }

        System.out.println("---------------------");
        System.in.read();
    }

}

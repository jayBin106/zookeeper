package com.demo.zookeeper.配置中心负载均衡;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.*;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.demo.zookeeper.zookeeper基本操作.ZookeeperDemo.zookeeperAddress;

/**
 * @author: wangsaichao
 * @date: 2018/9/30
 * @description: 服务1
 */
public class ProviderService1 {

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress,
                2000, 2000, new ExponentialBackoffRetry(1000, 3));
        client.start();
        client.blockUntilConnected();

        //服务构造器
        ServiceInstanceBuilder<InstanceDetails> sib = ServiceInstance.builder();
        //该服务中所有的接口
        Map<String, InstanceDetails.Service> services = new HashMap<>();

        // 添加订单服务接口
        //服务所需要的参数
        List<String> addOrderParams = new ArrayList<>();
        addOrderParams.add("createTime");
        addOrderParams.add("state");
        InstanceDetails.Service addOrderService = new InstanceDetails.Service();
        addOrderService.setDesc("添加订单");
        addOrderService.setMethodName("addOrder");
        addOrderService.setParams(addOrderParams);
        services.put("addOrder", addOrderService);


        //添加删除订单服务接口
        ArrayList<String> delOrderParams = new ArrayList<>();
        delOrderParams.add("orderId");
        InstanceDetails.Service delOrderService = new InstanceDetails.Service();
        delOrderService.setDesc("删除订单");
        delOrderService.setMethodName("delOrder");
        delOrderService.setParams(delOrderParams);
        services.put("delOrder", delOrderService);

        //服务的其他信息
        InstanceDetails payload = new InstanceDetails();
        payload.setServiceDesc("订单服务");
        payload.setServices(services);

        //将服务添加到 ServiceInstance
        ServiceInstance<InstanceDetails> orderService = sib.address("192.168.1.226")
                .port(8080)
                .name("OrderService")
                .payload(payload)
                .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                .build();

        //构建 ServiceDiscovery 用来注册服务
        ServiceDiscovery<InstanceDetails> serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                .client(client)
                .serializer(new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class))
                .basePath(InstanceDetails.ROOT_PATH)
                .build();
        //服务注册
        serviceDiscovery.registerService(orderService);
        serviceDiscovery.start();

        System.out.println("第一台服务注册成功......");

        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);

        serviceDiscovery.close();
        client.close();
    }

}

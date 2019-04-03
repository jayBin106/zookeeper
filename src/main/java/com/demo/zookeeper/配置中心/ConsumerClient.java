package com.demo.zookeeper.配置中心;

//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.curator.x.discovery.ServiceDiscovery;
//import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
//import org.apache.curator.x.discovery.ServiceInstance;
//import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
//
//import java.util.Collection;
//import java.util.Map;
//import java.util.Set;
//
//import static com.demo.zookeeper.zookeeper基本操作.ZookeeperDemo.zookeeperAddress;

/**
 * @author: wangsaichao
 * @date: 2018/9/30
 * @description:
 *
 * 加入依赖
 *
 *
 *   <!--<dependency>-->
 *             <!--<groupId>org.apache.curator</groupId>-->
 *             <!--<artifactId>curator-x-discovery</artifactId>-->
 *             <!--<version>${curator.version}</version>-->
 *             <!--<exclusions>-->
 *                 <!--<exclusion>-->
 *                     <!--<artifactId>curator-recipes</artifactId>-->
 *                     <!--<groupId>org.apache.curator</groupId>-->
 *                 <!--</exclusion>-->
 *             <!--</exclusions>-->
 *         <!--</dependency>-->
 *         <!--<dependency>-->
 *             <!--<groupId>org.springframework</groupId>-->
 *             <!--<artifactId>spring</artifactId>-->
 *             <!--<version>2.5.6.SEC03</version>-->
 *         <!--</dependency>-->
 *         <!--&lt;!&ndash; https://mvnrepository.com/artifact/hikari-cp/hikari-cp &ndash;&gt;-->
 *         <!--<dependency>-->
 *             <!--<groupId>hikari-cp</groupId>-->
 *             <!--<artifactId>hikari-cp</artifactId>-->
 *             <!--<version>1.8.1</version>-->
 *             <!--<exclusions>-->
 *                 <!--<exclusion>-->
 *                     <!--<artifactId>clojure</artifactId>-->
 *                     <!--<groupId>org.clojure</groupId>-->
 *                 <!--</exclusion>-->
 *             <!--</exclusions>-->
 *         <!--</dependency>-->
 *
 */
public class ConsumerClient {

//    public static void main(String[] args) throws Exception {
//        CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperAddress, new ExponentialBackoffRetry(1000, 3));
//        client.start();
//        client.blockUntilConnected();
//
//        ServiceDiscovery<InstanceDetails> serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
//                .client(client)
//                .basePath(InstanceDetails.ROOT_PATH)
//                .serializer(new JsonInstanceSerializer<InstanceDetails>(InstanceDetails.class))
//                .build();
//        serviceDiscovery.start();
//
//        boolean flag = true;
//
//        //死循环来不停的获取服务列表,查看是否有新服务发布
//        while (flag) {
//
//            //根据名称获取服务
//            Collection<ServiceInstance<InstanceDetails>> services = serviceDiscovery.queryForInstances("OrderService");
//            if (services.size() == 0) {
//                System.out.println("当前没有发现服务");
//                Thread.sleep(10 * 1000);
//                continue;
//            }
//
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
//            System.out.println("---------------------");
//            Thread.sleep(10 * 1000);
//
//        }

//    }

}

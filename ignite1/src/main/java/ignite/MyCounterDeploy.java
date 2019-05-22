package ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.ServiceResource;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/4/2
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/4/2     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class MyCounterDeploy {
    public void deploy(){
        Ignite ignite = Ignition.ignite();
        // Cluster group which includes all caching nodes.
        ClusterGroup cacheGrp = ignite.cluster().forCacheNodes("myCounterService");

// Get an instance of IgniteServices for the cluster group.
        IgniteServices svcs = ignite.services(cacheGrp);

// Deploy per-node singleton. An instance of the service
// will be deployed on every node within the cluster group.
        svcs.deployNodeSingleton("myCounterService", new MyCounterServiceImpl());
        svcs.deployClusterSingleton("myClusterSingleton", new MyCounterServiceImpl());
    }

    public String request() {
        Ignite ignite = Ignition.ignite();
        //分布式计算如果不指定集群组的话则会传播到所有节点
        IgniteCompute compute = ignite.compute(ignite.cluster().forAttribute("service.node", true));
//        IgniteCompute compute = ignite.compute(); //未部署服务的节点会抛出空指针
        compute.run(new IgniteRunnable() {
            @ServiceResource(serviceName = "myCounterService", proxySticky = false) //非粘性代理
            private MyCounterService counterService;

            @Override
            public void run() {
                int newValue = counterService.increment();
                System.out.println("Incremented value : " + newValue);
            }
        });
        return "all executed.";
    }
}

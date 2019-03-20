import com.alibaba.dubbo.common.URL;
import com.szkingdom.kjdp.register.zookeeper.transport.ChildListener;
import com.szkingdom.kjdp.register.zookeeper.transport.ZookeeperClient;
import com.szkingdom.kjdp.register.zookeeper.transport.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-09-13
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-09-13     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
class MyChildListener implements ChildListener{
  @Override
  public void childChanged(String s, List<String> list)
  {
      System.out.println("节点path:"+s);
      System.out.println("节点值:"+list.size());
  }

  @Override
  public void childDataChanged(String s, String s1)
  {
    System.out.println("data1:"+s);
    System.out.println("data2:"+s1);
  }
}
public class Test
{
  private static final String PATH = "/zk/test";
  static String connectString = "192.168.50.88:2181";
  public static void main(String[] args) throws Exception{

    testTree();

  }
  public static void test1() throws Exception{
    URL url = new URL(null,"192.168.50.88",2181) ;
    ZookeeperClient zk = new CuratorZookeeperClient(url);

    ChildListener listener = new MyChildListener();
    zk.addChildListener(PATH,listener);
    //zk.getDataUsingWatcher()
    sleep();
  }
  public static void testorginal()throws Exception{


    CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 3));
    client.start();
    PathChildrenCache cache = new PathChildrenCache(client, PATH, true);
    cache.start();
    PathChildrenCacheListener cacheListener = (client1, event) -> {
      System.out.println("事件类型：" + event.getType());
      if (null != event.getData()) {
        System.out.println("节点数据：" + event.getData().getPath() + " = " + new String(event.getData().getData()));
      }
    };
    cache.getListenable().addListener(cacheListener);
    sleep();
  }

  private static void sleep() throws InterruptedException
  {
    Thread.sleep(60*10000);
  }

  public static void testTree() throws Exception {

    CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 3));
    client.start();
//    client.create().creatingParentsIfNeeded().forPath(PATH);
    TreeCache cache = new TreeCache(client, PATH);
    TreeCacheListener listener = (client1, event) ->
        System.out.println("事件类型：" + event.getType() +
            " | 路径：" + (null != event.getData() ? event.getData().getPath() : null));
    cache.getListenable().addListener(listener);
    cache.start();
    sleep();
 /*   client.setData().forPath(PATH, "01".getBytes());
    Thread.sleep(100);
    client.setData().forPath(PATH, "02".getBytes());
    Thread.sleep(100);
    client.delete().deletingChildrenIfNeeded().forPath(PATH);
    Thread.sleep(1000 * 2);
    cache.close();
    client.close();
    System.out.println("OK!");*/
  }

}


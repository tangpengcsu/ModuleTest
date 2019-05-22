package parall;

import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import static org.mockito.Mockito.mock;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/4/28
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/4/28     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class Test {

    @org.junit.Test
    public void produer() throws InterruptedException{
        BlockingQueue<PCData> queue = new LinkedBlockingDeque<PCData>(10);
        Producer producer1 = new Producer(queue);
        Producer producer2 = new Producer(queue);
        Producer producer3 = new Producer(queue);
        Consumer consumer1 = new Consumer(queue);
        Consumer consumer2 = new Consumer(queue);
        Consumer consumer3 = new Consumer(queue);

        ExecutorService service = Executors.newCachedThreadPool();
        service.execute(producer1);
        service.execute(producer2);
        service.execute(producer3);
        service.execute(consumer1);
        service.execute(consumer2);
        service.execute(consumer3);
        Thread.sleep(10*1000);
        producer1.stop();
        producer2.stop();
        producer3.stop();
        Thread.sleep(3000);
        service.shutdown();
    }
    @org.junit.Test
    public void mocktest() {
        LinkedList mockList =mock(LinkedList.class);
        Mockito.when(mockList.get(0)).thenReturn("first");
        Mockito.when(mockList.get(1)).thenThrow(new IllegalArgumentException("not fit!"));
        System.out.println(mockList.get(0));

        System.out.println(mockList.get(1));
        System.out.println(mockList.get(0));


    }
}

package parall;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

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
public class Consumer implements Runnable {
    private static final int SLEEPTIME = 1000;
    private BlockingQueue<PCData> queue;

    public Consumer(BlockingQueue<PCData> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        System.out.println("start Consumer id = " + Thread.currentThread().getId());
        Random r = new Random();
        try {
            while (true) {
                PCData data = queue.take();
                if (null != data) {
                    int re = data.getData() * data.getData();

                    System.out.println(data.getData() + "*" + data.getData() + "=" + re);
                    Thread.sleep(r.nextInt(SLEEPTIME));
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}

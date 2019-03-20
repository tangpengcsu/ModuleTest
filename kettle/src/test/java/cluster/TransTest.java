package cluster;

import org. .After;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/16
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/16     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class TransTest {
    @Before
    public void init() throws Exception{
        KettleEnvironment.init();
    }
    @Test
    public void test(){

    }
    @After
    public void end() throws Exception{
        Thread.sleep(500000);
    }
}

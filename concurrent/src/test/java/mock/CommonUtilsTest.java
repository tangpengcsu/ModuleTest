package mock;


import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

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
public class CommonUtilsTest extends PowerMockTestCase {

    @Test
    public void verifyParametersTest() {
        Map<String,String> result = new HashMap<String,String>();
        //测试parameters为null
        Assert.assertEquals(CommonUtils.verifyParameters(result, null, "abc"), false);
        //测试keys为null
        Assert.assertEquals(CommonUtils.verifyParameters(result, "name:zhq,age:30",  null), false);
        //测试keys为""
        Assert.assertEquals(CommonUtils.verifyParameters(result, "name:zhq,age:30", ""), false);
        //测试parameters中的键值与keys不匹配
        Assert.assertEquals(CommonUtils.verifyParameters(result, "name:zhq,age:30", "name,height"), false);
        //测试parameters中的键值与keys不匹配
        Assert.assertEquals(CommonUtils.verifyParameters(result, "name:zhq,age:30", "name"), true);
        Assert.assertEquals(result.get("name"), "zhq");
    }
}

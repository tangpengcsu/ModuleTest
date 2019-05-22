package mock;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.Test;

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
@PrepareForTest({ CommonUtils.class })
public class InfoControllerTest extends PowerMockTestCase {

    //注入式的mock,用于真实调用
    @InjectMocks
    private InfoController infoController = new InfoController();

    //使用@Mock模拟调用对象中的函数
    @Mock
    private PersonQueryDao personDao;

    /**
     * 测试InfoController中的packageInfos
     */
    @Test
    public void packageInfosTest() {
        //在函数调用中使用CommonUtils类的静态方法需要先mockStatic一下CommonUtils
        PowerMockito.mockStatic(CommonUtils.class);
        //测试返回结果为空
        Mockito.when(CommonUtils.verifyParameters(Mockito.anyMapOf(String.class, String.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(false);
        Assert.assertEquals(infoController.packageInfos("name:zhq,age:30", "name", "age"), null);
        //测试返回结果非空
        //因为要模拟将CommonUtils.verifyParameters的结果放入Map中并返回,所以使用thenAnswer采用反射的方式将结果填入
        Mockito.when(CommonUtils.verifyParameters(Mockito.anyMapOf(String.class, String.class), Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenAnswer(new Answer<Boolean>() {
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                @SuppressWarnings("unchecked")
                Map<String,String> result = (Map<String,String>) invocation.getArguments()[0];
                result.put("name", "zhq");
                result.put("age", "30");
                return true;
            }
        });
        Map<String,String> result = infoController.packageInfos("name:zhq,age:30", "name", "age");
        Assert.assertEquals(result.get("name"), "zhq");
        Assert.assertEquals(result.get("age"), "30");
    }

    /**
     * 测试InfoController中的packageIntoPerson
     */
    @Test
    public void packageIntoPersonTest() {
        //测试返回null
        Mockito.when(personDao.query(Mockito.anyString(), Mockito.anyString())).thenReturn(null);
        Assert.assertEquals(infoController.packageIntoPerson("zhq", null), null);
        //测试返回非null
        Mockito.when(personDao.query(Mockito.anyString(), Mockito.anyString())).thenReturn(new PersonInfo());
        Assert.assertNotNull(infoController.packageIntoPerson("zhq", "age"));
    }

}
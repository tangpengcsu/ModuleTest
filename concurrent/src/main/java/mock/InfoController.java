package mock;

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
public class InfoController {
    /**
     * 负责将parameters中的key1:value1,key2:value2的数据解析并放置到result中
     * @param parameters
     * @param keys
     * @return
     */
    public Map<String, String> packageInfos(String parameters, String... keys) {
        Map<String, String> result = new HashMap<String, String>();
        if (CommonUtils.verifyParameters(result, parameters, keys)) {
            return result;
        }
        return null;
    }

    /**
     *
     * 负责判断参数的合法性并将参数内容封装到PersonInfo中
     * @param name
     * @param age
     * @return
     */
    public PersonInfo packageIntoPerson(String name, String age) {
        PersonQueryDao dao = new PersonQueryDao();
        return dao.query(name, age);
    }

}

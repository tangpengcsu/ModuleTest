package mock;



import org.apache.commons.lang.StringUtils;

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
public class CommonUtils {
    /**
     * 负责将parameters中的key1:value1,key2:value2的数据解析并放置到result中
     * @param result 解析后的结果Map
     * @param parameters 要解析的参数
     * @param keys 要解析的key
     * @return
     */
    public static boolean verifyParameters(Map<String, String> result,
                                           String parameters, String...keys) {
        Map<String, String> parameterMap = new HashMap<String, String>();
        if (StringUtils.isBlank(parameters) || keys == null || keys.length == 0) {
            return false;
        }
        String[] splitParameters = parameters.split(",");
        String oneSplitted[] = null;
        for (String splitted : splitParameters) {
            oneSplitted = splitted.split(":");
            if (oneSplitted.length < 2) continue;
            parameterMap.put(oneSplitted[0], oneSplitted[1]);
        }
        for (String key : keys) {
            if (!parameterMap.containsKey(key)) {
                return false;
            }
            result.put(key, parameterMap.get(key));
        }
        return true;
    }
}

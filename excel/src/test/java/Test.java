/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2018-04-27
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2018-04-27     1.0.1.0    tang.peng        创建
 * ----------------------------------------------------------------
 */
public class Test
{
  public static void main(String[] args){
    String str = "\"---\"---\"";
    System.out.println(str);
    String adStr = str.replaceAll("\"","\\\\\"");
    System.out.println(adStr);

    //System.out.println(Integer.valueOf("1"));
    //System.out.println(Float.valueOf("1.0").intValue());
  }
}

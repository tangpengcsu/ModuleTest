/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/15
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/15     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

public class TestNum extends AbstractJavaSamplerClient{

    private SampleResult results;

    /**
     * 输入的数字
     */
    private String inNum;

    /**
     * 需要匹配的数字
     */
    private String resultNum;

    /**
     * 初始化方法，初始化性能测试时的每个线程
     * 实际运行时每个线程仅执行一次，在测试方法运行前执行，类似于LoadRunner中的init方法
     */
    public void setupTest(JavaSamplerContext jsc) {
        results = new SampleResult();

        inNum = jsc.getParameter("inNum", "");
        resultNum = jsc.getParameter("resultNum", "");

        if (inNum != null && inNum.length() > 0){
            results.setSamplerData(inNum);
        }

        if (resultNum != null && resultNum.length() > 0){
            results.setSamplerData(resultNum);
        }
    }

    /**
     * 设置传入参数
     * 可以设置多个，已设置的参数会显示到Jmeter参数列表中
     */
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument("inNum","");
        params.addArgument("resultNum", "66");
        return params;
    }

    /**
     * 性能测试时的线程运行体
     * 测试执行的循环体，根据线程数和循环次数的不同可执行多次，类似于Loadrunner中的Action方法
     */
    public SampleResult runTest(JavaSamplerContext arg0) {
        boolean flag = false;
        //定义一个事务，表示这是事务的起始点，类似于Loadrunner中的lr.start_transaction
        results.sampleStart();

        for (int i = inNum.length();--i >= 0;){
            if (!Character.isDigit(inNum.charAt(i))){
                flag = false;
            }else{
                flag = true;
            }
        }

        for (int j = resultNum.length();--j >= 0;){
            if (!Character.isDigit(resultNum.charAt(j))){
                flag = false;
            }else{
                flag = true;
            }
        }
        //定义一个事务，表示这是事务的结束点，类似于Loadrunner中的lr.end_transaction
        results.sampleEnd();

        if (flag){
            Integer num = Integer.parseInt(inNum);
            Integer rsNum = Integer.parseInt(resultNum);

            if (num == rsNum){
                results.setDataEncoding("UTF-8");//因为响应的数据有中文，所以最好先设置编码
                results.setResponseData("恭喜你，答对了O(∩_∩)O~\n答案是【"+resultNum+"】");//响应数据，对应结果树，其他response code等可以自己点出来
                results.setSuccessful(true);//告诉系统返回正确还是错误
            } else if (num > rsNum){
                results.setDataEncoding("UTF-8");
                results.setResponseData("好像大了点~~~~(>_<)~~~~ \n您输入的是【"+inNum+"】");
                results.setSuccessful(false);
            }else {
                results.setDataEncoding("UTF-8");
                results.setResponseData("好像小了点~~~~(>_<)~~~~ \n您输入的是【"+inNum+"】");
                results.setSuccessful(false);
            }

        }else{
            results.setDataEncoding("UTF-8");
            results.setResponseData("请输入数字：~~~~(>_<)~~~~ \n您输入的inNum是【"+inNum+"】，resultNum是【"+resultNum+"】");
            results.setSuccessful(false);
        }

        return results;
    }

    /**
     * 测试结束方法，结束测试中的每个线程
     * 实际运行时，每个线程仅执行一次，在测试方法运行结束后执行，类似于Loadrunner中的End方法
     */
    public void teardownTest(JavaSamplerContext arg0) {
    }

}

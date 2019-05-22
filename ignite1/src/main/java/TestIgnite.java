import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.sql.*;

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
public class TestIgnite  extends AbstractJavaSamplerClient {


    private String type;
    private Integer step;
    private Integer startNum;
    private  Integer batchSize;
    /**
     * 设置传入参数
     * 可以设置多个，已设置的参数会显示到Jmeter参数列表中
     */
    public Arguments getDefaultParameters() {
        Arguments params = new Arguments();
        params.addArgument("type","write");
        params.addArgument("startNum", "0");
        params.addArgument("step","10000");
        params.addArgument("batchSize","50000");
        return params;
    }

    @Override
    public void setupTest(JavaSamplerContext context)  {
       String type =    context.getParameter("type", "write");
        results = new SampleResult();
        this.startNum =  context.getIntParameter("startNum", 0);
        this.step =  context.getIntParameter("step", 10000);
        this.batchSize =  context.getIntParameter("batchSize", 50000);
        this.type = type;
    }
    private SampleResult results;
    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        results.sampleStart();
        try {
            if("read".equals(type)){
                read();

            }else{
                write2();
            }
            results.setResponseData("success!!!");
            results.setSuccessful(true);
        } catch (Exception e) {
            e.printStackTrace();
            results.setResponseData("failed!!!");
            results.setErrorCount(1);

        }
        results.sampleEnd();
        results.setDataEncoding("UTF-8");//因为响应的数据有中文，所以最好先设置编码

        return results;
    }


    private   String jdbcName = "org.apache.ignite.IgniteJdbcThinDriver";
    private   Connection getConn() {
        try {
            Class.forName(jdbcName);
            System.out.println("加载驱动成功！");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("加载驱动失败！");
        }

        Connection con = null;
        try {
            //获取数据库连接

            con =DriverManager.getConnection("jdbc:ignite:thin://192.168.50.88/","","");
            //con = DriverManager.getConnection(dbUrl1, dbUserName, dbPassword);
            System.out.println("获取数据库连接成功！");
            System.out.println("进行数据库操作！");
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            System.out.println("获取数据库连接失败！");
        }
        return con;
    }

    public   void read() throws Exception{
        Connection con = getConn();

        String sql = "select * from test2";
        sql  ="SELECT STDDEV_POP(rec_sn) from SH_GH_20180904_410.SH_GH_20180904_410 group by rec_sn";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement)con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            int col = rs.getMetaData().getColumnCount();
            System.out.println("============================");
            while (rs.next()) {
             /*   for (int i = 1; i <= col; i++) {
                    System.out.print(rs.getString(i) + "\t");
                    if ((i == 2) && (rs.getString(i).length() < 8)) {
                        System.out.print("\t");
                    }
                }*/
                System.out.println("");
            }
            System.out.println("============================");
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            close(con);
        }

    }
    public static void main(String[] args) throws Exception{
        new TestIgnite().read();
    }


    public   int write2(){
        Connection conn = getConn();
        int i = 0;
        String sql = "insert into test2 (\n" +
                "f1  ,\n" +
                "f2 ,\n" +
                "f3 ,\n" +
                "f4 ,\n" +
                "f5 ,\n" +
                "f6 ,\n" +
                "f7 ,\n" +
                "f8 ,\n" +
                "f9 ,\n" +
                "f10 ,\n" +
                "f11 ,\n" +
                "f12 ,\n" +
                "f13 ,\n" +
                "f14 ,\n" +
                "f15 ,\n" +
                "f16 ,\n" +
                "f17 ,\n" +
                "f18 ,\n" +
                "f19 ,\n" +
                "f20 ,\n" +
                "f21 ,\n" +
                "f22 ,\n" +
                "f23 ,\n" +
                "f24 ,\n" +
                "f25 ,\n" +
                "f26 ,\n" +
                "f27 ,\n" +
                "f28 ,\n" +
                "f29 ,\n" +
                "f30 ) values (\n" +
                "  ? ,\n" +
                "  ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                " ?,\n" +
                "  ?,\n" +
                "  ?,\n" +
                " ?," +
                "  ?," +
                "  ?," +
                "  ?," +
                " ?,"+
                "  ?," +
                "  ?," +
                "  ?," +
                "  ?," +
                " ?," +
                " ?," +
                "  ?," +
                "  ?," +
                " ?," +
                " ?," +
                " ?," +
                " ?," +
                " ?," +
                " ?," +
                " ?," +
                " ?," +
                " ?, " +
                " ?)";
        PreparedStatement pstmt;

        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);

            int num = startNum+1;
            int sum = startNum + step;
            while(num<sum){
                pstmt.setString(1,"f1_"+num);
                pstmt.setString(2, "f2_"+num);
                pstmt.setString(3, "f3_"+num);
                pstmt.setString(4, "f4_"+num);
                pstmt.setString(5, "f5_"+num);
                pstmt.setString(6, "f6_"+num);
                pstmt.setString(7, "f7_"+num);
                pstmt.setString(8, "f8_"+num);
                pstmt.setString(9, "f9_"+num);
                pstmt.setString(10, "f10_"+num);
                pstmt.setString(11, "f11_"+num);
                pstmt.setString(12, "f12_"+num);
                pstmt.setString(13, "f13_"+num);
                pstmt.setString(14, "f14_"+num);
                pstmt.setString(15, "f15_"+num);
                pstmt.setString(16, "f16_"+num);
                pstmt.setString(17, "f17_"+num);
                pstmt.setString(18, "f18_"+num);
                pstmt.setString(19, "f19_"+num);

                pstmt.setString(20, "f20_"+num);
                pstmt.setString(21, "f21_"+num);
                pstmt.setString(22, "f22_"+num);
                pstmt.setString(23, "f23_"+num);
                pstmt.setString(24, "f24_"+num);
                pstmt.setString(25, "f25_"+num);
                pstmt.setString(26, "f26_"+num);
                pstmt.setString(27, "f27_"+num);
                pstmt.setString(28, "f28_"+num);
                pstmt.setString(29, "f29_"+num);
                pstmt.setString(30, "f30_"+num);

                pstmt.addBatch();

                if ((num + 1) % batchSize == 0) {
                    System.out.println("开始写入...");

                    pstmt.executeBatch();
                    System.out.println("写入完成！"+num +"->"+sum );
                }
                // i = pstmt.executeUpdate();
                num++;
            }

 /*           System.out.println("开始写入...");
           // pstmt.executeBatch();

            System.out.println("写入完成！");*/
            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }
    private   void close( Connection con) throws Exception{
        con.close();

    }
}

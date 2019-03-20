import java.sql.*;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/14
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/14     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class Test  {

    private static String jdbcName = "org.apache.ignite.IgniteJdbcThinDriver";
    private static Connection getConn() {
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

    public static void read() throws Exception{
        Connection con = getConn();

        String sql = "select * from test2";
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
    public static int write(){
        Connection conn = getConn();
        int i = 0;
        String sql = "insert into test1 (f1,f2) values(?,?)";
        PreparedStatement pstmt;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);
            int num = 0;
            while(num<1000000){
                pstmt.setString(1,"f1_"+num);
                pstmt.setString(2, "f2_"+num);
                pstmt.addBatch();
               // i = pstmt.executeUpdate();
                num++;
            }
            pstmt.executeBatch();

            pstmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return i;
    }

    public static int write2(){
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
        int batchSize = 50000;
        try {
            pstmt = (PreparedStatement) conn.prepareStatement(sql);

            int num = 0;
            while(num<1000000){
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
                    System.out.println("写入完成！");
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

    public static void main(String[] args) throws Exception {

        long start = System.currentTimeMillis();
         // write();
           read();
        //write2();
        Long time = (System.currentTimeMillis() - start)/1000;
        System.out.println("耗时："+time+"秒！");
    }

    private static void close( Connection con) throws Exception{
        con.close();

    }
}
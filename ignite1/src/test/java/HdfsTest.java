import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.util.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/18
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/18     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class HdfsTest {
    FileSystem fileSystem;
    String inputFileNmae = "/ml/wc.input5";
    Path path;

    @Before
    public void init() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://SparkMaster:9000/");
        fileSystem = FileSystem.get(configuration);
        path = new Path(inputFileNmae);

    }

    @Test
    public void write() throws IOException {
        if (fileSystem.exists(path)) {
            System.out.println("准备删除旧文件："+inputFileNmae);
            distory();
        }

        Block block1 = new Block(1L, 24L, 1L,"zhangs");

        FSDataOutputStream outStream = fileSystem.create(path);
        block1.write(outStream);

    }

    @Test
    public void read() throws Exception {
        FSDataInputStream fsis = fileSystem.open(path);
        Block b = new Block();
        b.readFields(fsis);
        System.out.println(b);

    }


    @Test
    public void test2() throws IOException {
        //第一个参数为block的Id，第二个为分配的长度，第三个戳,共三个标志
        //此Block为自定义块，而不是使用hadoop自带的。可以看后面
        Block block1 = new Block(1L, 24L, 1L);
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout1 = new DataOutputStream(bout);
        //序列化对象到IO流中
        block1.write(dout1);
        System.out.println("hadoop序列化长度（三个long）:" + bout.size());
    }

    @Test
    public void test1() throws FileNotFoundException, IOException,
            ClassNotFoundException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bout);
        out.writeLong(1L);
        out.writeLong(1L);
        out.writeLong(1L);

        out.close();
        System.out.println("java序列化长度（三个long）:" + bout.size());//java序列化长度（三个long）:30
    }

    @org.junit.Test
    public void distory() {

        System.out.println("开始删除文件：" + inputFileNmae);
        try {

            fileSystem.delete(path, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("删除文件：" + inputFileNmae + "结束！");
        }

    }
}

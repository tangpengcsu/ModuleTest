import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.util.IOUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
public class Test {
    FileSystem fileSystem;
    String inputFileNmae = "/ml/wc.input2";
    Path path;

    @Before
    public void init() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://SparkMaster:9000/");
        fileSystem = FileSystem.get(configuration);
        path = new Path(inputFileNmae);

    }

    @org.junit.Test
    public void write() throws Exception {
        if (fileSystem.exists(path)) {
            System.out.println("准备删除旧文件："+inputFileNmae);
            distory();
        }
        System.out.println("开始写");
        //Initializes The Object
        User1 user = new User1();
        user.setName("hollis");
        user.setAge(23);

        ObjectOutputStream oos =
                new ObjectOutputStream(new FSDataOutputStream(fileSystem.create(path)));
        oos.writeObject(user);
        oos.close();
        System.out.println("写结束！");
    }

    @org.junit.Test
    public void read() throws Exception {
        System.out.println("开始读");

        ObjectInputStream ois = new ObjectInputStream(new FSDataInputStream(fileSystem.open(path)));
        User1 newUser = (User1) ois.readObject();
        System.out.println(newUser);
        System.out.println("读结束");
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

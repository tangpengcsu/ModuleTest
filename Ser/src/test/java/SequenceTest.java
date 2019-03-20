import com.szkingdom.fspt.define.datastruct.senddata.DtsStkCycleOrderDefine;
import kryo.Simple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/19
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/19     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class SequenceTest {

    FileSystem fileSystem;
    // String dirctory = "/tmp/kk/test/obj/order";
    String dirctory = "/ml/spark-dts";
    String defaultFs = "hdfs://SparkMaster:9000/";
    Path path;
    Configuration conf;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://10.80.1.242:8020/");
        conf.set("fs.defaultFS", defaultFs);
        fileSystem = FileSystem.get(conf);

        path = new Path(dirctory);

    }

    @Test
    public void test() throws Exception {
        int count = 0;
        Long start = System.currentTimeMillis();
        List<String> files = getFiles();
        for (String file : files) {
            Path path = new Path(file);
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));//路径
            NullWritable key = NullWritable.get();
            BytesWritable value = new BytesWritable();
            while (reader.next(key, value)) {
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value.getBytes()));
                DtsStkCycleOrderDefine[] simples = (DtsStkCycleOrderDefine[]) ois.readObject();
                for (int i = 0; i < simples.length; i++) {
                   //System.out.println(simples[i].m_lRecSn());
                    count++;
                }
                ois.close();
            }
        }
        System.out.println(files.size() + "个文件，共读取" + count + "条数据，耗费：" + (System.currentTimeMillis() - start) + "毫秒！");

    }

    public List<String> getFiles() throws Exception {
        FileStatus[] fileStatuses =
                fileSystem.listStatus(path);

        List<String> files = Arrays.stream(fileStatuses).filter(v -> v.isFile() && !(v.getPath().getName().equals(
                "_SUCCESS"))).map(i -> {
            return dirctory + "/" + i.getPath().getName();
        }).collect(Collectors.toList());
        return files;
    }
}

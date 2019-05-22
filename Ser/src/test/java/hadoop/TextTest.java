package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 版权声明：本程序模块属于大数据分析平台（KDBI）的一部分
 * 金证科技股份有限公司 版权所有
 * <p>
 * 模块名称：${DESCRIPTION}
 * 模块描述：${DESCRIPTION}
 * 开发作者：tang.peng
 * 创建日期：2019/3/20
 * 模块版本：1.0.1.0
 * ----------------------------------------------------------------
 * 修改日期        版本        作者          备注
 * 2019/3/20     1.0.1.0       tang.peng     创建
 * ----------------------------------------------------------------
 */
public class TextTest {
    FileSystem fileSystem;
    // String inputFileNmae = "/tmp/kk/test/obj/order";
    String   inputFileNmae = "/ml/spark-test";
    Path path;
    Configuration conf;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://10.80.1.242:8020/");
        conf.set("fs.defaultFS", "hdfs://SparkMaster:9000/");
        fileSystem = FileSystem.get(conf);

        path = new Path(inputFileNmae);

    }

}

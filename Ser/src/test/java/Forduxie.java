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

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

//for循环读写操作演示
public class Forduxie {
    public static void main(String args[]) throws Exception {
        final Path path = new Path("/sf1");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://SparkMaster:9000/");

        final FileSystem fs = FileSystem.get(new URI("hdfs://SparkMaster:9000/ml/"), conf);

        @SuppressWarnings("deprecation")
        final SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,path, LongWritable.class,Text.class);
        for (int i = 0; i < 10; i++) {
            writer.append(new LongWritable(i), new Text(i+"=_="));
        }
        IOUtils.closeStream(writer);

        @SuppressWarnings({ "deprecation" })
        final SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,conf);
        LongWritable key = new LongWritable();
        Text val = new Text();
        while (reader.next(key, val)) {
            System.out.println(key.get() + "\t" + val.toString());
        }IOUtils.closeStream(reader);
    }
}
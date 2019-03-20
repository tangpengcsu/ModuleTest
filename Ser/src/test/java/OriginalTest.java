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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.szkingdom.fspt.define.datastruct.senddata.DtsStkCycleOrderDefine;
import kryo.Simple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class OriginalTest {
    FileSystem fileSystem;
    //String dirctory = "/FSPT/CLEARING_DATABASE/DATA/SEND/20190118/100013/DTS_STK_CYCLE_ORDER";
    String   inputFileNmae = "/ml/spark-test";

    Path path;
    Configuration configuration;

    @Before
    public void init() throws Exception {
          configuration = new Configuration();
        //configuration.set("fs.defaultFS", "hdfs://10.80.1.242:8020/");
        configuration.set("fs.defaultFS", "hdfs://SparkMaster:9000/");
        fileSystem = FileSystem.get(configuration);
        path = new Path(inputFileNmae);

    }

    @Test
    public void count() throws Exception{
        serializable();
        deSerializable();

        //java原生序列化时间:1584 ms
        //java原生反序列化时间:1647 ms
    }
    @Test
    public void serializable() throws IOException {

        long start =  System.currentTimeMillis();
        int sum = 100000;
        ObjectOutputStream so = new ObjectOutputStream(fileSystem.create(path));

        for (int i = 0; i < sum; i++) {
            Map<String, Integer> map = new HashMap<String, Integer>(2);
            map.put("zhang0", i);
            map.put("zhang1", i);
            so.writeObject(new Simple("zhang" + i, (i + 1), map));
        }
        so.flush();
        so.close();
        System.out.println(sum+"条数据，java原生序列化时间:" + (System.currentTimeMillis() - start) + " ms" );
    }
    @Test
    public void files() throws Exception {
        FileStatus[] fileStatuses =
                fileSystem.listStatus(path);

        List<String> fisl = Arrays.stream(fileStatuses).filter(v -> v.isFile() && !(v.getPath().getName().equals(
                "_SUCCESS"))).map(i -> {
           /* System.out.println(i.getPath());
            System.out.println(i.getPath().getName());*/
            return i.getPath().toString().replace("hdfs://10.80.1.242:8020", "");
        }).collect(Collectors.toList());
        for(String file :fisl){
            long start =  System.currentTimeMillis();
            int i = 0;
            try {

                ObjectInputStream si = new ObjectInputStream(fileSystem.open(new Path(file)));
               Object obj =  si.readObject();
                DtsStkCycleOrderDefine simple = null;
                while ((simple = (DtsStkCycleOrderDefine) si.readObject()) != null) {
                      //  System.out.println(simple);
                    i++;
                }

                si.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                 e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            System.out.println(i+"条数据，java原生反序列化时间:" + (System.currentTimeMillis() - start) + " ms" );
        }
    }
    @Test
    public void deSerializable() {
        long start =  System.currentTimeMillis();
        int i = 0;
        try {

            ObjectInputStream si = new ObjectInputStream(fileSystem.open(path));

            Simple simple = null;
            while ((simple = (Simple) si.readObject()) != null) {
           //     System.out.println(simple);
                i++;
            }

            si.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            //e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(i+"条数据，java原生反序列化时间:" + (System.currentTimeMillis() - start) + " ms" );

    }


    @Test
    public void testse() throws Exception{
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setMaxDepth(20);

        Path seqFile=new Path(inputFileNmae+"/part-00001");
//Reader内部类用于文件的读取操作
        SequenceFile.Reader reader=new SequenceFile.Reader(fileSystem,seqFile,configuration);

        NullWritable key =  NullWritable.get();
        BytesWritable value = new BytesWritable();
        // Object o = reader.next(key,value);
        while(reader.next(key,value)){
        /*    Input input = new Input(value.getBytes());
            InputStream inputStream = new ByteArrayInputStream(value.getBytes());*/
            ObjectInputStream si = new ObjectInputStream(new ByteArrayInputStream(value.getBytes()));
            Object simple = null;
            while ((simple =   si.readObject()) != null) {
                //     System.out.println(simple);
                System.out.println(simple);
            }

            si.close();
        }

        // System.out.println(key);

        System.out.println(new String(value.getBytes()));
    }

}
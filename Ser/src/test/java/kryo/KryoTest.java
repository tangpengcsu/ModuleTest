package kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.szkingdom.fspt.define.datastruct.senddata.DtsStkCycleOrderDefine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.serializer.KryoSerializationStream;
import org.junit.Before;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

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
public class KryoTest {
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

    @Test
    public void count() throws Exception {
        serializable();
        deSerializable();
        //  Kryo 序列化时间:983 ms
        //Kryo 反序列化时间:660 ms
    }

    @Test
    public void serializable() throws Exception {

        long start = System.currentTimeMillis();
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(Simple.class);
        Output output = new Output(fileSystem.create(path));
        int sum = 100000;
        for (int i = 0; i < sum; i++) {
            Map<String, Integer> map = new HashMap<String, Integer>(2);
            map.put("zhang0", i);
            map.put("zhang1", i);
            kryo.writeObject(output, new Simple("zhang" + i, (i + 1), map));
        }
        output.flush();
        output.close();
        System.out.println(sum + "条数据，Kryo 序列化时间:" + (System.currentTimeMillis() - start) + " ms");

        //FSDataOutputStream outStream = fileSystem.create(path);
    }


    @Test
    public void doRead() throws Exception {
        long start = System.currentTimeMillis();

        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        Input input;
        int i = 0;
        try {


            HdfsDataInputStream hdis = (HdfsDataInputStream) fileSystem.open(path);


            List<LocatedBlock> allbck = hdis.getAllBlocks();
            for (LocatedBlock block : allbck) {
                System.out.println(block);
            }
            FSDataInputStream fsis = fileSystem.open(path);

            input = new Input(fsis);
            DtsStkCycleOrderDefine simple = null;

            while ((simple = kryo.readObject(input, DtsStkCycleOrderDefine.class)) != null) {
                //  System.out.println(simple);
                System.out.println(simple.m_chMarket() + "-" + simple.m_iTrdDate());
                i++;
            }

            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (KryoException e) {

        }
        System.out.println(i + "条数据，Kryo 反序列化时间:" + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void test11() throws Exception {
        ObjectInputStream ois = new ObjectInputStream(new FSDataInputStream(fileSystem.open(new Path("/part-00000"))));
        DtsStkCycleOrderDefine sample_model = (DtsStkCycleOrderDefine) ois.readObject();
        System.out.println(sample_model.m_strTrdacct());
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

        for (String file : fisl) {
            Kryo kryo = new Kryo();
            kryo.setReferences(false);
            kryo.setRegistrationRequired(false);
            kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
            Input input;
            int i = 0;
            try {
                FSDataInputStream fsis = fileSystem.open(new Path(file));
                input = new Input(fsis, 10000);
                DtsStkCycleOrderDefine simple = null;
                Object obj = kryo.readObject(input, DtsStkCycleOrderDefine.class);

                while ((simple = kryo.readObject(input, DtsStkCycleOrderDefine.class)) != null) {
                    System.out.println("out:" + simple.m_strTrdId() + simple.m_iTrdDate());
                    i++;
                }

                input.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (KryoException e) {
                e.printStackTrace();
            }
            System.out.println(file + "文件中读取" + i + "条数据！");
        }

        //  System.out.println(fisl);
     /*   for(FileStatus status:fileStatuses){
            process(status);
        }*/
    }

    public void process(
            FileStatus fileStatus) throws Exception {
        if (fileStatus.isFile()) {
            System.out.println("--------------");
            System.out.println(
                    fileStatus.getAccessTime());  //上次访问的时间
            System.out.println(
                    fileStatus.getOwner());  //文件的所有者
            System.out.println(
                    fileStatus.getGroup());  //文件的所属者
            System.out.println(
                    fileStatus.getPath());  //得到文件的路径
            System.out.println(
                    fileStatus.getPermission());  //文件的权限
            System.out.println(
                    fileStatus.getReplication());  //文件的备份数
        } else if (fileStatus.isDirectory()) {

            // 和Java的File不一样的地方：
            // 当File对象所代表的是目录的时候，
            // 可以通过listFiles方法来获取该目录下的所有文件（有可能还包含目录）

            // 在HDFS中，当FileStatus对象代表一个目录的时候
            // 没有相应的方法来获取该目录下的所有文件
            // 要通过FileSystem类来获取该目录下的文件
            //      path=fileStatus.getPath();
            //      FileStatus[] fileStstuses=
            //          fs.listStatus(path);
            FileStatus[] fileStatuses =
                    fileSystem.listStatus(fileStatus.getPath());
            for (FileStatus status : fileStatuses) {
                process(status);
            }
        }
    }

    @Test
    public void deSerializable() throws Exception {
        long start = System.currentTimeMillis();

        Kryo kryo = new Kryo();
        kryo.setReferences(false);
/*        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new SerializingInstantiatorStrategy());
        kryo.register(Simple.class);*/



  /*      kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.setRegistrationRequired(false);*/
        // kryo.setRegistrationRequired(true);
        //kryo.register(Simple.class,67);
        kryo.register(Simple.class);
        //kryo.register(Map.class,41);
        kryo.setMaxDepth(20);
        Input input;
        int i = 0;
        try {
            FSDataInputStream fsis = fileSystem.open(new Path(inputFileNmae + "/part-00001"));
            input = new Input(fsis);
            Simple simple = null;
            JavaSerializer javaSerializer = new JavaSerializer();
            //  Object sim=   javaSerializer.read(kryo,input,Simple.class);
            while ((simple = kryo.readObject(input, Simple.class)) != null) {
                System.out.println(simple);
                i++;
            }

            input.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (KryoException e) {
            e.printStackTrace();
        }
        System.out.println(i + "条数据，Kryo 反序列化时间:" + (System.currentTimeMillis() - start) + " ms");
    }


    @Test
    public void testSimple() throws Exception {



        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setMaxDepth(20);
        /*   kryo.register(Simple.class,13994);*/
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());

        Path seqFile = new Path(inputFileNmae + "/part-00000");
//Reader内部类用于文件的读取操作
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(seqFile));//路径

//                SequenceFile.Reader reader=new SequenceFile.Reader(fileSystem,seqFile,conf);


        NullWritable key = NullWritable.get();
        BytesWritable value = new BytesWritable();
        // Object o = reader.next(key,value);

        while (reader.next(key, value)) {


           /* String name =input.readString();
            int age = input.readInt();
            System.out.println("name:"+name+", age"+ age);*/
//            System.out.println("class:"+kryo.readClass(input));
            // System.out.println("class&object:"+kryo.readClassAndObject(input));


            //  System.out.println(simple);

//            System.out.println("key:"+key+", value:"+new String(value.getBytes()));

         /*   Simple simple = null;
            while((simple=kryo.readObject(input, Simple.class)) != null){
                System.out.println(simple);

            }*/

            //   System.out.println("key:"+key+", value:"+new String(value.getBytes()));
            //        byte[] bytevalue = value.getBytes();
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value.getBytes()));
            Object ob = ois.readObject();
            Simple[] simples = (Simple[]) ob;
            Arrays.stream(simples).forEach(i -> System.out.println(i));

            //   Input input = new Input(value.getBytes());

            //  Simple simple =kryo.readObject(input,Simple.class);

  /*          Simple simple =null;
            JavaSerializer javaSerializer = new JavaSerializer();
            //  Object sim=   javaSerializer.read(kryo,input,Simple.class);
            while((simple=kryo.readObject(input, Simple.class)) != null){
                System.out.println(simple);
             //   i++;
            }*/

            // System.out.println(simple);

        }


        // System.out.println(key);


    }

    @Test
    public void testDtsStkCycleOrderDefine() throws Exception {
        /*Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.80.1.242:8020/");*/
        Path seqFile = new Path("/tmp/kk/test/obj/order/part-00001");
//Reader内部类用于文件的读取操作
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(seqFile));//路径

        NullWritable key = NullWritable.get();
        BytesWritable value = new BytesWritable();

        while (reader.next(key, value)) {
            //System.out.println("key:"+key+", value:"+new String(value.getBytes()));
            //        byte[] bytevalue = value.getBytes();
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value.getBytes()));
            Object ob = ois.readObject();
            DtsStkCycleOrderDefine[] simples = (DtsStkCycleOrderDefine[]) ob;
            Arrays.stream(simples).forEach(i -> {
                System.out.println(i.m_strTrdacct() + "-" + i.m_iTrdDate() + "-" + i.m_strBoard() + ":" + i);
            });
        }

    }
}

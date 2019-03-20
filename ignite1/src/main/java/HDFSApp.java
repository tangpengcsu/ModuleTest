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

import java.io.*;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class HDFSApp {

    public static FileSystem getFileSystem() throws Exception{
        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS", "hdfs://SparkMaster:9000/");
        FileSystem fileSystem=FileSystem.get(configuration);
        return fileSystem;
    }
    //read
    public static void read() throws Exception{
        FileSystem fileSystem=getFileSystem();
        String fileName="/ml/wc.input";
        Path path=new Path(fileName);
        FSDataInputStream inStream=fileSystem.open(path);
        try{
            IOUtils.copyBytes(inStream, System.out, 4096, false);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
    //write
    public static void write() throws Exception{
        FileSystem fileSystem=getFileSystem();
        //output fileName
        String outputFileName="E:\\Users\\Documents\\大数据分析平台\\kettle\\1.ktr";
        //input fileName
        String inputFileNmae="/ml/wc.input";
        Path path=new Path(inputFileNmae);
        FSDataOutputStream outStream=fileSystem.create(path);


        FileInputStream inStream=new FileInputStream(new File(outputFileName));
        try{
            IOUtils.copyBytes(inStream, outStream, 4096, false);
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(inStream);
            IOUtils.closeStream(outStream);
        }

    }
   static String inputFileNmae="/ml/wc.input2";
    public static void readObject() throws Exception{
        FileSystem fileSystem=getFileSystem();
        Path path=new Path(inputFileNmae);
        ObjectInputStream  ois= new ObjectInputStream(new FSDataInputStream(fileSystem.open(path)));
        User1 newUser = (User1) ois.readObject();
        System.out.println(newUser);

    }
    public static void writeObject() throws Exception{
        FileSystem fileSystem=getFileSystem();
        //Initializes The Object
        User1 user = new User1();
        user.setName("hollis");
        user.setAge(23);

        Path path=new Path(inputFileNmae);
        ObjectOutputStream oos =
                new ObjectOutputStream(new FSDataOutputStream(fileSystem.create(path)));
        oos.writeObject(user);
        oos.close();

      /*  Path path=new Path(inputFileNmae);
        FSDataOutputStream outStream=fileSystem.create(path);

        Student student=new Student();
        student.setId(new IntWritable(10));
        student.setName(new Text("Lance"));
        student.setGender(true);

        ByteArrayOutputStream baos=
                new ByteArrayOutputStream();
        DataOutputStream dos=
                new DataOutputStream(baos);
        student.write(dos);
        byte[] data=baos.toByteArray();
        System.out.println(Arrays.toString(data));
        System.out.println(data.length);
        outStream.write(data);*/
        //outStream.write(user);

    }
    public static void main(String[] args) throws Exception {
     // read();
        //   write();
        writeObject();
         readObject();

    }

}

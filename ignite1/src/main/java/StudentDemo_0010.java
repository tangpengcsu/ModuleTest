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
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StudentDemo_0010{
    public static void main(String[] args) throws Exception{
/*        Student student=new Student();
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
        System.out.println(data.length);*/
write();

        // 将data进行反序列化？
    }
    public static void read()throws  Exception{

    }
    public static void write() throws Exception{
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
      //  student.readFields();
    }
}

class Student implements Writable{
    private IntWritable id;
    private Text name;
    private boolean gender;
    private List<Text> list=new ArrayList<>();

    Student(){
        id=new IntWritable();
        name=new Text();
    }

    /**
     *
     * @param student
     */
    Student(Student student){
        // 在Hadoop中这属于引用复制，完全杜绝这种现象
        //this.id=student.id;
        //this.name=student.name;
        // 在Hadoop中要使用属性值的复制
        id=new IntWritable(student.id.get());
        name=new Text(student.name.toString());
    }

    @Override
    public void write(DataOutput out) throws IOException{
        id.write(out);
        name.write(out);
        BooleanWritable gender=
                new BooleanWritable(this.gender);
        gender.write(out);
        // 在Hadoop中序列化Java中所对应的集合的时候，
        // 应该现将集合的长度进行序列化，然后将集合中的
        // 每一个元素进行序列化
        int size=list.size();
        new IntWritable(size).write(out);
        for(int i=0;i<size;i++){
            Text text=list.get(i);
            text.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        id.readFields(in);
        name.readFields(in);
        BooleanWritable bw=new BooleanWritable();
        bw.readFields(in);
        gender=bw.get();
        // 在反序列化集合的时候应该先反序列化集合的长度
        IntWritable size=new IntWritable();
        size.readFields(in);
        // 再反序列化流中所对应的结合中的每一个元素
        list.clear();
        for(int i=0;i<size.get();i++){
            Text text=new Text();
            text.readFields(in);
            list.add(text);// 此步骤有没有问题？？？
        }
    }


    public IntWritable getId(){
        return id;
    }

    public void setId(IntWritable id){
        this.id=id;
    }

    public Text getName(){
        return name;
    }

    public void setName(Text name){
        this.name=name;
    }

    public boolean isGender(){
        return gender;
    }

    public void setGender(boolean gender){
        this.gender=gender;
    }

    public List<Text> getList(){
        return list;
    }

    public void setList(List<Text> list){
        this.list=list;
    }
}


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
class Block implements Writable {
    private long blockId;
    private long numBytes;
    private long stamp;
    private String name;

    public Block() {
    }

    public Block(long blockId, long numBytes, long stamp) {
        super();
        this.blockId = blockId;
        this.numBytes = numBytes;
        this.stamp = stamp;
    }

    public Block(long blockId, long numBytes, long stamp, String name) {
        this.blockId = blockId;
        this.numBytes = numBytes;
        this.stamp = stamp;
        this.name = name;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
     /*   int newLength = WritableUtils.readVInt(in);
        byte[] bytes = new byte[newLength];
        in.readFully(bytes, 0, newLength);
        this.name = new String(bytes, "UTF-8");*/
        blockId = in.readLong();
        numBytes = in.readLong();
        System.out.println("name"+name);
        System.out.println("blockId"+blockId);

        stamp = in.readLong();



    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeLong(blockId);
        out.writeLong(numBytes);
        out.writeLong(stamp);
    }

    @Override
    public String toString() {
        return "Block{" +
                "blockId=" + blockId +
                ", numBytes=" + numBytes +
                ", stamp=" + stamp +
                ", name='" + name + '\'' +
                '}';
    }
}
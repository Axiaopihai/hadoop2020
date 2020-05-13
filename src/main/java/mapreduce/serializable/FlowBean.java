package mapreduce.serializable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zxq
 * 2020/4/28
 */
public class FlowBean implements Writable {

    private int upFlow;
    private int downFlow;
    private int sum;

    //反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBean() {
        super();
    }

    public void set(int upFlow, int downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sum = upFlow+downFlow;
    }
    //序列化和反序列化方法数据类型和顺序必须一致,且各值不能为null，否则报空指针
    //序列化方法，用于写出
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.upFlow);
        dataOutput.writeInt(this.downFlow);
        dataOutput.writeInt(this.sum);
    }

    //反序列化方法，用于读入
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.downFlow=dataInput.readInt();
        this.sum=dataInput.readInt();
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sum=" + sum +
                '}';
    }
}

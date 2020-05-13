package mapreduce.customcomparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zxq
 * 2020/5/2
 */
public class CustomComparable implements WritableComparable<CustomComparable> {

    private int upFlow;
    private int downFlow;
    private int sum;


    @Override
    public int compareTo(CustomComparable o) {
        if (sum>o.getSum()){
            return -1;
        }else if (sum<o.getSum()){
            return 1;
        }else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(sum);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow=dataInput.readInt();
        this.downFlow=dataInput.readInt();
        this.sum=dataInput.readInt();
    }

    public CustomComparable() {
    }

    public void set(int upFlow, int downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sum = upFlow+downFlow;
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
        return
                upFlow +"\t"+
                downFlow +"\t"+
                sum;
    }
}

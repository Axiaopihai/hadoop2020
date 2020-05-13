package mapreduce.join.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 */
public class RJoinBean implements Writable {
    private String orderId;
    private String pId;
    private int amount;
    private String pName;
    private String flag;

    public void set(String orderId, String pId, int amount, String pName, String flag) {
        this.orderId = orderId;
        this.pId = pId;
        this.amount = amount;
        this.pName = pName;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pName);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId=dataInput.readUTF();
        this.pId=dataInput.readUTF();
        this.amount=dataInput.readInt();
        this.pName=dataInput.readUTF();
        this.flag=dataInput.readUTF();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return orderId + "\t" + pName + "\t" + amount;
    }
}

package mapreduce.reduce.groupcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zxq
 * 2020/5/2
 */
public class GroupBean implements WritableComparable<GroupBean> {

    private Integer id;
    private String pid;
    private Double money;

    @Override
    public int compareTo(GroupBean o) {
        if (id>o.getId()){
            return 1;
        }else if (id<o.getId()){
            return -1;
        }else {
            return money>o.getMoney()?1:-1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeDouble(money);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id=dataInput.readInt();
        this.pid=dataInput.readUTF();
        this.money=dataInput.readDouble();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    @Override
    public String toString() {
        return "GroupBean{" +
                "id='" + id + '\'' +
                ", pid='" + pid + '\'' +
                ", money=" + money +
                '}';
    }
}

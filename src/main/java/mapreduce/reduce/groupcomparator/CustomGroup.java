package mapreduce.reduce.groupcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author zxq
 * 2020/5/11
 */
public class CustomGroup extends WritableComparator {


    public CustomGroup() {
        super(GroupBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        GroupBean a1 = (GroupBean) a;
        GroupBean b1 = (GroupBean) b;
        //只按id进行分组
        if (a1.getId()>b1.getId()){
            return -1;
        }else if (a1.getId()<b1.getId()){
            return 1;
        }else {
            return 0;
        }

    }
}

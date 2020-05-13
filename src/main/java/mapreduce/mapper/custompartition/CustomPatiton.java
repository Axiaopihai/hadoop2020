package mapreduce.mapper.custompartition;

import mapreduce.serializable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zxq
 * 2020/5/2
 */
public class CustomPatiton extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String key = text.toString();
        if (key.startsWith("136")){
            return 0;
        }else if (key.startsWith("137")){
            return 1;
        }else if (key.startsWith("138")){
            return 2;
        }else {
            return 3;
        }
    }
}

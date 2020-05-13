package mapreduce.serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author zxq
 * 2020/4/28
 */
public class PhoneReduce extends Reducer<Text,FlowBean,Text,FlowBean> {


    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int downFlowSum=0;
        int upFlowSum=0;
        for (FlowBean data : values) {
            int downFlow = data.getDownFlow();
            int upFlow = data.getUpFlow();
            downFlowSum+=downFlow;
            upFlowSum+=upFlow;
        }
        flowBean.set(upFlowSum,downFlowSum);
        context.write(key,flowBean);

    }
}

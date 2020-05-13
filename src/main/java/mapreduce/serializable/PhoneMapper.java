package mapreduce.serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zxq
 * 2020/4/28
 */
public class PhoneMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String strValue = value.toString();
        String[] splits = strValue.split("\t");
        String phoneNum = splits[1];

        int upFlow = Integer.parseInt(splits[splits.length - 3]);
        int downFlow = Integer.parseInt(splits[splits.length - 2]);

        text.set(phoneNum);
        flowBean.set(upFlow,downFlow);
        context.write(text,flowBean);
    }
}

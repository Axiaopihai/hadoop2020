package mapreduce.customcomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/2
 */
public class ComparableMapper extends Mapper<LongWritable,Text,CustomComparable,Text> {

    private CustomComparable customComparable = new CustomComparable();
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String strValue = value.toString();
        String[] splits = strValue.split("\t");
        String phoneNum = splits[0];

        int upFlow = Integer.parseInt(splits[1]);
        int downFlow = Integer.parseInt(splits[2]);

        customComparable.set(upFlow,downFlow);
        text.set(phoneNum);

        context.write(customComparable,text);

    }
}

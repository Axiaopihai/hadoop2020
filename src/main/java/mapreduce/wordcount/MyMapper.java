package mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author zxq
 * 2020/4/27
 */
public class MyMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,IntWritable> {

    private Text text = new Text();
    private IntWritable intWritable = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String strValue = value.toString();
        String[] splits = strValue.split(" ");
        for (String split:splits) {
            text.set(split);
            context.write(text,intWritable);
        }
    }
}

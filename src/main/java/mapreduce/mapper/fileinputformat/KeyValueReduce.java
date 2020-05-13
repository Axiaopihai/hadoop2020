package mapreduce.mapper.fileinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zxq
 * 2020/4/28
 */
public class KeyValueReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable intWritable = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int  count=0;
        for (IntWritable value : values) {
            count+=value.get();
        }
        intWritable.set(count);
        context.write(key,intWritable);
    }
}

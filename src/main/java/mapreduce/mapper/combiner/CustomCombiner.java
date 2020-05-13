package mapreduce.mapper.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/2
 */
public class CustomCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable intWritable = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value :values) {
            int i = value.get();
            sum+=i;
        }
        intWritable.set(sum);
        context.write(key,intWritable);
    }
}

package mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author zxq
 * 2020/4/27
 */
public class MyReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable sum = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            IntWritable one = iterator.next();
            int i = one.get();
            count+=i;
        }
        sum.set(count);
        context.write(key,sum);
    }
}

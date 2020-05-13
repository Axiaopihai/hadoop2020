package mapreduce.counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/12
 */
public class CounterMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        String[] splits = str.split(" ");
        if (splits.length>11){
            //getCounter("计数器组","计数器名称")
            //getCounter("枚举项")
            context.getCounter("map","trueCount").increment(1);
            text.set(str);
            context.write(text,NullWritable.get());
        }else {
            context.getCounter("map","falseCount").increment(1);

        }

    }
}

package mapreduce.customcomparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/2
 */
public class ComparableReduce extends Reducer<CustomComparable,Text,Text,CustomComparable> {

    @Override
    protected void reduce(CustomComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }

    }
}

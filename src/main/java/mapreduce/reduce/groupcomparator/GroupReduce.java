package mapreduce.reduce.groupcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 */
public class GroupReduce extends Reducer<GroupBean,NullWritable,GroupBean,NullWritable> {

    @Override
    protected void reduce(GroupBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}

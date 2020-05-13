package mapreduce.mapper.fileinputformat.customfileinputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/2
 */
public class CustomInputReduce extends Reducer<Text,BytesWritable,Text,BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for (BytesWritable value : values) {
            context.write(key,value);
        }
    }
}

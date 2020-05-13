package mapreduce.reduce.fileoutputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 */
public class FileOutputReduce extends Reducer<Text,NullWritable,Text,NullWritable> {
    private Text text = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String str = key.toString();
        String s = str + "\n";
        text.set(s);
        context.write(text,NullWritable.get());
    }
}

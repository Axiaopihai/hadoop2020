package mapreduce.mapper.fileinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zxq
 * 2020/4/28
 */
public class KeyValuleMapper extends Mapper<Text,Text,Text,IntWritable> {

    private IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {


        context.write(key,intWritable);

    }
}

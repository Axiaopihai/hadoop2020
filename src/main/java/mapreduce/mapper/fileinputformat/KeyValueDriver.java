package mapreduce.mapper.fileinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zxq
 * 2020/4/28
 * 指定mapper阶段输入
 */
public class KeyValueDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //设置key和value分隔符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");

        Job job = Job.getInstance(conf);

        job.setJarByClass(KeyValueDriver.class);

        job.setMapperClass(KeyValuleMapper.class);
        job.setReducerClass(KeyValueReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //设置输入方式
        //KeyValueTextInputFormat修改了默认输入mapper的数据
        //CombineFileInputFormat和NLineInputFormat修改了默认分片逻辑
        job.setInputFormatClass(KeyValueTextInputFormat.class);

//        job.setInputFormatClass(CombineFileInputFormat.class);
//        CombineFileInputFormat.setMaxInputSplitSize(job,1024);
//
//        job.setInputFormatClass(NLineInputFormat.class);
//        NLineInputFormat.setNumLinesPerSplit(job,4);

        //job.submit();
        job.waitForCompletion(true);
    }
}

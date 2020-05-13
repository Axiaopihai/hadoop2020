package mapreduce.reduce.fileoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 * 自定义输出类型
 */
public class FileOutputDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(FileOutputDriver.class);
        //关联mapper和reduce类
        job.setMapperClass(FileOutputMapper.class);
        job.setReducerClass(FileOutputReduce.class);
        //设置map阶段输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输入输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置输出类型
        job.setOutputFormatClass(CustomFileOutput.class);
        //提交job
//        job.submit();
        job.waitForCompletion(true);
    }

}

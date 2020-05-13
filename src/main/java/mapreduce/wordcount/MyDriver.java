package mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zxq
 * 2020/4/27
 * wordcount简单案例
 */
public class MyDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        System.setProperty("hadoop.home.dir","D:\\apps\\hadoop\\hadoop-2.7.2");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(MyDriver.class);
        //关联mapper和reduce类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        //设置map阶段输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置最终输入输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
//        job.submit();
        job.waitForCompletion(true);
    }
}

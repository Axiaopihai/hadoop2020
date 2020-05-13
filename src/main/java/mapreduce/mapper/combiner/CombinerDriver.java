package mapreduce.mapper.combiner;

import mapreduce.wordcount.MyDriver;
import mapreduce.wordcount.MyMapper;
import mapreduce.wordcount.MyReduce;
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
 * 2020/5/2
 * 开启combiner功能
 */
public class CombinerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(CombinerDriver.class);
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
        //设置combiner
        job.setCombinerClass(CustomCombiner.class);
        //应用combiner注意不能影响逻辑的正确性，是reduce之前进行了部分的汇总，
        //相当于进行了局部的reduce，所以可以直接应用MyReduce类，节省了网络io
//        job.setCombinerClass(MyReduce.class);


        //提交job
//        job.submit();
        job.waitForCompletion(true);
    }
}

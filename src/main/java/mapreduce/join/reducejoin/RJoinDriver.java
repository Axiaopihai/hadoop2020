package mapreduce.join.reducejoin;

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
 * 在reduce端进行join，mapper端主要对数据打标签，标明数据来源，reduce端进行join操作
 */
public class RJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(RJoinDriver.class);
        //关联mapper和reduce类
        job.setMapperClass(RJoinMapper.class);
        job.setReducerClass(RJoinReduce.class);
        //设置map阶段输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(RJoinBean.class);
        //设置最终输入输出类型
        job.setOutputKeyClass(RJoinBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交job
//        job.submit();
        job.waitForCompletion(true);
    }
}

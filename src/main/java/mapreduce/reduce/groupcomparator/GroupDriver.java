package mapreduce.reduce.groupcomparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 * 自定义分组
 */
public class GroupDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包路径
        job.setJarByClass(GroupDriver.class);
        //关联mapper和reduce类
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReduce.class);
        //设置map阶段输入输出类型
        job.setMapOutputKeyClass(GroupBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置最终输入输出类型
        job.setOutputKeyClass(GroupBean.class);
        job.setOutputValueClass(NullWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //设置分组类
        job.setGroupingComparatorClass(CustomGroup.class);
        //提交job
//        job.submit();
        job.waitForCompletion(true);
    }
}

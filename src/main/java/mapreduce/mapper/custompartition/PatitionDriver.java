package mapreduce.mapper.custompartition;

import mapreduce.serializable.FlowBean;
import mapreduce.serializable.PhoneMapper;
import mapreduce.serializable.PhoneReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/2
 * 自定义分区
 */
public class PatitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(PatitionDriver.class);

        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //指定分区
        job.setPartitionerClass(CustomPatiton.class);
        //指定reducetask的数量，数量默认为1，不指定分区输出到一个文件，大于1小于分区时，
        //报io异常，大于分区时，正常，当有空闲资源浪费。
        job.setNumReduceTasks(4);

        //job.submit();
        job.waitForCompletion(true);
    }
}

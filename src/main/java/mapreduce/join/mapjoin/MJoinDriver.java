package mapreduce.join.mapjoin;

import mapreduce.join.reducejoin.RJoinBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author zxq
 * 2020/5/12
 * mapper端进行join，缓存小表到内存中，读取大表进行操作。没有reduce阶段。
 */
public class MJoinDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(MJoinDriver.class);

        job.setMapperClass(MJoinMapper.class);

        job.setMapOutputKeyClass(RJoinBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(RJoinBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //指定加载小表位置
        job.addCacheFile(new URI("file:///D:/tmp/input/joininput/pd.txt"));
        //没有reduce阶段，设置reduce阶段task为0
        job.setNumReduceTasks(0);

        //job.submit();
        job.waitForCompletion(true);

    }


}

package mapreduce.reduce.groupcomparator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 */
public class GroupMapper extends Mapper<LongWritable,Text,GroupBean,NullWritable> {


    private GroupBean groupBean = new GroupBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //0000001	Pdt_01	222.8
        String str = value.toString();
        String[] splits = str.split("\t");
        groupBean.setId(Integer.parseInt(splits[0]));
        groupBean.setPid(splits[1]);
        groupBean.setMoney(Double.parseDouble(splits[2]));

        context.write(groupBean,NullWritable.get());

    }
}

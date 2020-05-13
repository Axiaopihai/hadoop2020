package mapreduce.join.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 */
public class RJoinMapper extends Mapper<LongWritable,Text,Text,RJoinBean> {

    private RJoinBean rJoinBean = new RJoinBean();
    private Text text = new Text();
    private String name;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        name = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1001	01	1
        String str = value.toString();
        String[] splits = str.split("\t");
        if (name.startsWith("order")){
            rJoinBean.set(splits[0],splits[1],Integer.parseInt(splits[2]),"",name);
            text.set(splits[1]);
        }else {
            rJoinBean.set("",splits[0],0,splits[1],name);
            text.set(splits[0]);
        }

        context.write(text,rJoinBean);

    }
}

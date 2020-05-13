package mapreduce.join.mapjoin;

import mapreduce.join.reducejoin.RJoinBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author zxq
 * 2020/5/12
 */
public class MJoinMapper extends Mapper<LongWritable,Text,RJoinBean,NullWritable> {

    private HashMap<String, String> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"utf-8"));
        String line;
        while (StringUtils.isNotEmpty(line=bufferedReader.readLine())){
            String[] splits = line.split("\t");
            map.put(splits[0],splits[1]);
        }
        bufferedReader.close();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String str = value.toString();
        String[] splits = str.split("\t");
        RJoinBean rJoinBean = new RJoinBean();
        rJoinBean.set(splits[0],splits[1],Integer.parseInt(splits[2]),map.get(splits[1]),"");
        context.write(rJoinBean,NullWritable.get());

    }

}

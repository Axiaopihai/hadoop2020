package mapreduce.join.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;


/**
 * @author zxq
 * 2020/5/11
 */
public class RJoinReduce extends Reducer<Text, RJoinBean, RJoinBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<RJoinBean> values, Context context) throws IOException, InterruptedException {
        HashMap<String, String> map = new HashMap<>();
        ArrayList<RJoinBean> rJoinBeans = new ArrayList<>();
        for (RJoinBean value : values) {
            String flag = value.getFlag();
            if (flag.startsWith("pd")) {
                map.put(value.getpId(), value.getpName());
            } else if (flag.startsWith("order")){
                RJoinBean dest = new RJoinBean();
                try {
                    BeanUtils.copyProperties(dest,value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                rJoinBeans.add(dest);
            }
        }
        for (RJoinBean rJoinBean : rJoinBeans) {
            rJoinBean.setpName(map.get(rJoinBean.getpId()));
            context.write(rJoinBean, NullWritable.get());
        }
    }
}

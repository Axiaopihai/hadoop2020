package mapreduce.reduce.fileoutputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 */
public class MyRecordWriter extends RecordWriter<Text,NullWritable> {

    private FSDataOutputStream oneOutStream;
    private FSDataOutputStream twoOutStream;

    public void myInit(TaskAttemptContext taskAttemptContext){
        try {
            FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
            Path onePahe = new Path("D:\\tmp\\output\\one.log");
            Path twoPath = new Path("D:\\tmp\\output\\two.log");
            oneOutStream = fs.create(onePahe);
            twoOutStream = fs.create(twoPath);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String str = text.toString();
        if (str.contains("atguigu")){
            oneOutStream.write(str.getBytes());
        }else {
            twoOutStream.write(str.getBytes());
        }

    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(oneOutStream);
        IOUtils.closeStream(twoOutStream);
    }
}

package mapreduce.mapper.fileinputformat.customfileinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author zxq
 * 2020/4/28
 */
public class MyRecordReader extends RecordReader<Text,BytesWritable> {

    private Configuration configuration;
    private FileSplit fileSplit;
    private boolean flag=true;
    private BytesWritable bytesWritable = new BytesWritable();
    private Text text = new Text();
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        configuration = taskAttemptContext.getConfiguration();
        //强转是因为输入的类型为文件，还有其他实现类
        this.fileSplit= (FileSplit) inputSplit;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //主要的操作

        if (flag){
            //获取操作文件的path及文件系统
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fs.open(path);
            //获取文件长度，拷贝数据

            byte[] bytes = new byte[(int) fileSplit.getLength()];
            IOUtils.readFully(inputStream,bytes,0,bytes.length);
            bytesWritable.set(bytes,0,bytes.length);

            String name = fileSplit.getPath().getName();
            text.set(name);

            flag=false;
            //返回true表明一次处理的数据读取完毕，相当于读取了一条数据，可进行下一步处理
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return text;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}

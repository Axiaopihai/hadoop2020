package mapreduce.reduce.fileoutputformat;


import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zxq
 * 2020/5/11
 */
public class CustomFileOutput extends FileOutputFormat {
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        MyRecordWriter myRecordWriter = new MyRecordWriter();
        myRecordWriter.myInit(taskAttemptContext);
        return myRecordWriter;
    }
}

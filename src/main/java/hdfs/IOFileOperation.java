package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author zxq
 * 2020/4/23
 */
public class IOFileOperation {

    private FileSystem getFs() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        java.net.URI uri = new java.net.URI("hdfs://192.168.159.128:9000");
        return FileSystem.get(uri, conf, "root");
    }

    @Test
    //上传，下载文件
    public void downLoad() throws InterruptedException, IOException, URISyntaxException {
        FileSystem fs = this.getFs();
        //获取输入和输出流
        FSDataInputStream inputStream = fs.open(new Path("/hadoop"));
        FileOutputStream outputStream = new FileOutputStream(new File("E:\\hadoop.txt"));
        //流对拷
        IOUtils.copyBytes(inputStream,outputStream,new Configuration());
        //关闭资源
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
        fs.close();
    }

    @Test
    public void seekLoad() throws InterruptedException, IOException, URISyntaxException {
        FileSystem fs = this.getFs();
        //获取输入和输出流
        FSDataInputStream inputStream = fs.open(new Path("/hadoop"));
        //从指定的位置开始下载
        inputStream.seek(1024);
        FileOutputStream outputStream = new FileOutputStream(new File("E:\\hadoop.txt"));
        byte[] bytes = new byte[1024];
        //指定下载长度
        for (int i=1024;i<1024*5;i++){
            inputStream.read(bytes);
            outputStream.write(bytes);
        }
        //关闭资源
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
        fs.close();

    }

}

package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * @author zxq
 * 2020/4/23
 */
public class FileOperation {

    private FileSystem getFs() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        java.net.URI uri = new java.net.URI("hdfs://192.168.159.128:9000");
        return FileSystem.get(uri, conf, "root");
    }


    @Test
    public void upload() throws InterruptedException, IOException, URISyntaxException {
        FileSystem fs = this.getFs();
        //文件上传,相似操作：文件下载，文件删除，文件更名
//        fs.copyFromLocalFile(new Path("E:\\hadoop.txt"),new Path("/hadoop"));
        boolean delete = fs.delete(new Path("/tmp"), true);
        System.out.println(delete);
        fs.close();
    }

    @Test
    public void status() throws InterruptedException, IOException, URISyntaxException {
        FileSystem fs = this.getFs();
        //获取文件信息
        FileStatus[] status = fs.listStatus(new Path("/"));
        for (FileStatus statue : status) {
            System.out.println(statue.isFile());
            System.out.println(statue.getPath().getName());
            System.out.println(statue.getLen());
            System.out.println(statue.getOwner());
            System.out.println(statue.getGroup());
        }
        //获取文件信息，信息更全
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),true);
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.isDirectory());
            System.out.println(fileStatus.getPath().getName());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation location : blockLocations) {
                String[] hosts = location.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
    }




}

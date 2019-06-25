package HaoXin.LearFlink.Core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @ClassName: Hdfs
 * @Author: 郝鑫
 * @Data: 2019/6/25/19:51
 * @Descripition:
 */
public class Hdfs {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://haoxin01:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("hdsf://haoxin01:8020/haoxin/data"));
        for (FileStatus fileStatus :fileStatuses) {
            System.out.println(fileStatus.getPath().getName());
        }


    }
}

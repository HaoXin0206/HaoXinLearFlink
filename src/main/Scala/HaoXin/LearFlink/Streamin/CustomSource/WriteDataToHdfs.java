package HaoXin.LearFlink.Streamin.CustomSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.runtime.TraitSetter;

import java.beans.Transient;


/**
 * @ClassName: WriteDataToHdfs
 * @Author: 郝鑫
 * @Data: 2019/7/6/8:27
 * @Descripition:
 */

public class WriteDataToHdfs extends RichSinkFunction<ReadMySource.Info> {

    private org.apache.hadoop.conf.Configuration conf;
    private FileSystem fileSystem;
    private FSDataOutputStream append;
    private String hdfsPath = "/haoxin/flinkdata/20190706";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", "hdfs://haoxin01:8020");
        conf.set("dfs.replication","1");
        conf.setBoolean("dfs.support.append", true);
        fileSystem = FileSystem.get(conf);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null != append) {
            append.close();
        }
        if (null != fileSystem) {
            fileSystem.close();
        }
    }

    @Override
    public void invoke(ReadMySource.Info value, Context context) throws Exception {

        append = fileSystem.append(new Path(hdfsPath));
        append.writeBytes(value.time() + "\t" + value.name() + "\t" + value.phon());
        append.flush();

    }
}

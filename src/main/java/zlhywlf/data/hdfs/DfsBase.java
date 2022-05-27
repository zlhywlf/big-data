package zlhywlf.data.hdfs;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import zlhywlf.data.base.IJob;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author zlhywlf
 */
public abstract class DfsBase implements IJob {

    @Override
    public void driver(String[] args) {
        System.out.printf("parameters:%s\n", Arrays.toString(args));
        try (FileSystem fs = FileSystem.get(new Configuration())) {
            driver(args, fs);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 操作
     *
     * @param args 参数
     * @param fs   FileSystem
     */
    abstract void driver(String[] args, FileSystem fs);


}

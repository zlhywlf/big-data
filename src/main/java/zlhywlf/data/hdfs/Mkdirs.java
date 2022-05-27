package zlhywlf.data.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class Mkdirs extends DfsBase {

    @Override
    void driver(String[] args, FileSystem fs) {
        try {
            String path = args[1];
            fs.mkdirs(new Path(path));
            System.out.printf("Successfully created %s\n", path);
        } catch (IOException | ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }
}

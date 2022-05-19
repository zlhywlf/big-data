package zlhywlf.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @author zlhywlf
 */
public class LogCollector implements Runnable {
    static Logger log = LoggerFactory.getLogger(LogCollector.class);

    @Override
    public void run() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String today = formatter.format(LocalDate.now());
        log.info(today);
        File logs = new File("logs");
        File[] uploadFiles = logs.listFiles((dir, name) -> name.startsWith("access_"));


        File tmp = new File("tmp");
        boolean isExist = tmp.exists();
        if (!isExist) {
            isExist = tmp.mkdirs();
        }

        if (isExist && uploadFiles != null) {
            for (File file : uploadFiles) {
                boolean b = file.renameTo(new File(tmp.getPath() + "/" + file.getName()));
                log.info("file.renameTo {}", b);

            }
        }

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9001");
        try(FileSystem fs = FileSystem.get(configuration)) {

            Path to = new Path("/collect_log"+today);
            if (!fs.exists(to)) {
                fs.mkdirs(to);
            }
            File[] files = tmp.listFiles();
            if (files != null) {
                for (File file : files) {
                    fs.copyFromLocalFile(new Path(file.getPath()),new Path("/collect_log"+today+"/"+file.getName()));
                }
            }
            log.info(today);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

package zlhywlf.data.upload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author zlhywlf
 */
public class LogCollector implements Runnable {
    static Logger log = LoggerFactory.getLogger(LogCollector.class);

    @Override
    public void run() {
        // 获取配置
        Properties prop = null;
        try {
            prop = PropTool.getProp();
            log.info("获取配置={}", prop);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (prop != null) {
            // 获取当前日期
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            String today = formatter.format(LocalDate.now());
            log.info("日期={}", today);

            // 日志目录扫描指定前缀的文件
            File logs = new File(prop.getProperty(Constant.LOGS_DIR));
            final String prefix = prop.getProperty(Constant.LOG_PREFIX);
            File[] uploadFiles = logs.listFiles((dir, name) -> name.startsWith(prefix));
            log.info("日志文件数量 {}", uploadFiles != null ? uploadFiles.length : "null");
            if (uploadFiles == null || uploadFiles.length == 0) {
                log.info("无日志需要上传");
                return;
            }

            // 将日志转存到临时目录
            File tmp = new File(prop.getProperty(Constant.LOG_TMP_FOLDER));
            boolean isExist = tmp.exists();
            if (!isExist) {
                isExist = tmp.mkdirs();
            }
            if (isExist) {
                for (File file : uploadFiles) {
                    boolean b = file.renameTo(new File(tmp.getPath() + "/" + file.getName()));
                    log.info("移动 {} {}", file.getName(), b ? "成功" : "失败");
                }
            }

            // hadoop上传
            Configuration configuration = new Configuration();
            // 如果不配置就是在本地操作
            configuration.set("fs.defaultFS", "hdfs://localhost:9001");
            String hPath = prop.getProperty(Constant.HDFS_TARGET_FOLDER) + "_" + today;
            try (FileSystem fs = FileSystem.get(configuration)) {
                Path to = new Path(hPath);
                if (!fs.exists(to)) {
                    fs.mkdirs(to);
                }
                File[] files = tmp.listFiles();
                if (files != null) {
                    for (File file : files) {
                        fs.copyFromLocalFile(new Path(file.getPath()), new Path(hPath + "/" + file.getName()));
                        log.info("上传 {}", file.getName());
                    }
                }
                log.info("上传完毕");
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            log.warn("配置为null");
        }
    }
}

interface Constant {
    String LOGS_DIR = "LOGS.DIR";
    String LOG_PREFIX = "LOG.PREFIX";
    String LOG_TMP_FOLDER = "LOG.TMP.FOLDER";
    String HDFS_TARGET_FOLDER = "HDFS.TARGET.FOLDER";
}

class PropTool {

    private static volatile Properties prop = null;


    public static Properties getProp() throws IOException {
        if (prop == null) {
            synchronized ("lock") {
                if (prop == null) {
                    prop = new Properties();
                    prop.load(PropTool.class.getClassLoader()
                            .getResourceAsStream("collector.properties"));
                }
            }
        }

        return prop;
    }

}
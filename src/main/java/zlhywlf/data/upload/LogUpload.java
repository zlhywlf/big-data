package zlhywlf.data.upload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zlhywlf
 */
public class LogUpload {
    public static void run(){
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("schedule-pool-%d").daemon(true).build();
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1, threadFactory);
        executorService.schedule(new LogCollector(), 0, TimeUnit.SECONDS);
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

package zlhywlf.data.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import zlhywlf.data.base.IJob;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public abstract class JobBase implements IJob {

    @Override
    public void driver(String[] args) {
        try {
            Job job = Job.getInstance(new Configuration(), this.getClass().getSimpleName());
            driver(args, job);
            FileInputFormat.setInputPaths(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException | ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }

    /**
     * 操作
     *
     * @param args 参数
     * @param job  job
     */
    abstract void driver(String[] args, Job job);


}

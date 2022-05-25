package zlhywlf.data.speak;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class SpeakDriver {
    public static void driver(String inputPath, String outputPath){
        final Configuration conf = new Configuration();
        try {
            final Job job = Job.getInstance(conf, "speakDriver");
            job.setJarByClass(SpeakDriver.class);

            job.setMapperClass(SpeakMapper.class);
            job.setReducerClass(SpeakReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(SpeakBean.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(SpeakBean.class);

            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

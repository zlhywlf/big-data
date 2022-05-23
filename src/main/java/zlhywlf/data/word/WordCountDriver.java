package zlhywlf.data.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class WordCountDriver {

    public static void driver(String inputPath, String outputPath) {
        Configuration configuration = new Configuration();
        // 如果不配置就是在本地操作
        configuration.set("fs.defaultFS", "hdfs://localhost:9001");
        try {
            Job job = Job.getInstance(configuration, "WordCountDriver");
            // driver
            job.setJarByClass(WordCountDriver.class);
            // mapper
            job.setMapperClass(WordCountMapper.class);
            // reduce
            job.setReducerClass(WordCountReducer.class);
            // mapper output
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // reduce output
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // input
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            // output
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            // commit
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

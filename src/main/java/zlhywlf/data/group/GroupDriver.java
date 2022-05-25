package zlhywlf.data.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class GroupDriver {
    public static void driver(String inputPath, String outputPath) {
        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "GroupDriver");
            job.setJarByClass(GroupDriver.class);
            job.setMapperClass(GroupMapper.class);
            job.setReducerClass(GroupReducer.class);
            job.setMapOutputKeyClass(GroupBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(GroupBean.class);
            job.setOutputValueClass(NullWritable.class);
            job.setPartitionerClass(GroupPartitioner.class);
            // key 相同判断逻辑
            job.setGroupingComparatorClass(GroupComparator.class);
            job.setNumReduceTasks(2);
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

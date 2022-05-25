package zlhywlf.data.partition;

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
public class PartitionDriver {
    public static void driver(String inputPath, String outputPath){
        final Configuration conf = new Configuration();
        try {
            final Job job = Job.getInstance(conf);
            job.setJarByClass(PartitionDriver.class);
            job.setMapperClass(PartitionMapper.class);
            job.setReducerClass(PartitionReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(PartitionBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PartitionBean.class);

            // 4 设置使用自定义分区器
            job.setPartitionerClass(CustomPartitioner.class);
            //5 指定reduce task的数量与分区数量保持一致,分区数量是3
            job.setNumReduceTasks(3);

            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

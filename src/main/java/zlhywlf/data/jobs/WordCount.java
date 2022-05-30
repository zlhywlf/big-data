package zlhywlf.data.jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class WordCount extends JobBase {
    @Override
    void driver(String[] args, Job job) {
        // driver
        job.setJarByClass(WordCount.class);
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
    }

    static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final Text word = new Text();
        private final IntWritable count = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.printf("map### key=%s,value=%s\n", key, value);
            String[] words = value.toString().split(" ");
            for (String s : words) {
                word.set(s);
                context.write(word, count);
            }
        }
    }

    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                System.out.printf("reduce### key=%s,value=%s\n", key, value);
                sum += value.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }
}




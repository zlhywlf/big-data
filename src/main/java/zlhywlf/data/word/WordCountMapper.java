package zlhywlf.data.word;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LongWritable 文本偏移量,Text 一行文本
 * Text 单词,IntWritable 数量
 *
 * @author zlhywlf
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text word = new Text();
    private final IntWritable count = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String s : words) {
            word.set(s);
            context.write(word, count);
        }
    }
}

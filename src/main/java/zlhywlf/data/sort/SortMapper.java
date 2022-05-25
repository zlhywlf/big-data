package zlhywlf.data.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author zlhywlf
 */
public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {
    private final SortBean bean = new SortBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 读取一行文本，转为字符串，切分
        final String[] fields = value.toString().split("\t");
        //2 解析出各个字段封装成SpeakBean对象
        bean.setDeviceId(fields[0]);
        bean.setSelfDuration(Long.parseLong(fields[1]));
        bean.setThirdPartDuration(Long.parseLong(fields[2]));
        bean.setSumDuration(Long.parseLong(fields[4]));
        //3 SpeakBean作为key输出
        context.write(bean, NullWritable.get());
    }
}

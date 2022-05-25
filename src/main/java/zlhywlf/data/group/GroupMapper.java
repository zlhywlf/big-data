package zlhywlf.data.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class GroupMapper extends Mapper<LongWritable, Text, GroupBean, NullWritable> {
    private final GroupBean bean = new GroupBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] fields = value.toString().split("\t");
        bean.setOrderId(fields[0]);
        bean.setPrice(Double.parseDouble(fields[2]));
        context.write(bean, NullWritable.get());
    }
}

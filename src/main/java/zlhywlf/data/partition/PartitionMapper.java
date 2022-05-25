package zlhywlf.data.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {
    private final PartitionBean bean = new PartitionBean();
    private final Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] fields = value.toString().split("\t");
        String appKey = fields[2];

        bean.setId(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppKey(fields[2]);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThirdPartDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);

        k.set(appKey);
        context.write(k, bean);
    }
}

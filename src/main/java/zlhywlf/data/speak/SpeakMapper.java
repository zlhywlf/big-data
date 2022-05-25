package zlhywlf.data.speak;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class SpeakMapper extends Mapper<LongWritable, Text, Text, SpeakBean> {
    private final Text deviceIdGroup = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();
        final String[] fields = line.split("\t");
        String selfDuration = fields[fields.length - 3];
        String thirdPartDuration = fields[fields.length - 2];
        String deviceId = fields[1];
        final SpeakBean bean = new SpeakBean(Long.parseLong(selfDuration), Long.parseLong(thirdPartDuration), deviceId, Long.parseLong(selfDuration) + Long.parseLong(thirdPartDuration));
        deviceIdGroup.set(deviceId);
        context.write(deviceIdGroup, bean);
    }
}

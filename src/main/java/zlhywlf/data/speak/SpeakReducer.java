package zlhywlf.data.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class SpeakReducer extends Reducer<Text, SpeakBean, Text, SpeakBean> {
    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        Long selfDurationSum = 0L;
        Long thirdPartDurationSum = 0L;
        for (SpeakBean bean : values) {
            final Long selfDuration = bean.getSelfDuration();
            final Long thirdPartDuration = bean.getThirdPartDuration();
            selfDurationSum += selfDuration;
            thirdPartDurationSum += thirdPartDuration;
        }
        final SpeakBean bean = new SpeakBean(selfDurationSum, thirdPartDurationSum, key.toString(), selfDurationSum + thirdPartDurationSum);
        context.write(key, bean);
    }
}

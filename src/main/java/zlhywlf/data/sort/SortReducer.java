package zlhywlf.data.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zlhywlf
 */
public class SortReducer extends Reducer<SortBean, NullWritable, SortBean, NullWritable> {
    @Override
    protected void reduce(SortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, value);
        }
    }
}

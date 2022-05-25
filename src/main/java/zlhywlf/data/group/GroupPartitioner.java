package zlhywlf.data.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zlhywlf
 */
public class GroupPartitioner extends Partitioner<GroupBean, NullWritable> {
    @Override
    public int getPartition(GroupBean groupBean, NullWritable nullWritable, int i) {
        return (groupBean.getOrderId().hashCode() & Integer.MAX_VALUE) % i;
    }
}

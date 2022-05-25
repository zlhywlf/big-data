package zlhywlf.data.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author zlhywlf
 */
public class CustomPartitioner extends Partitioner<Text, PartitionBean> {
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int i) {
        int partition;

        if (text.toString().equals("kar")) {
            //只需要保证满足此if条件的数据获得同个分区编号集合
            partition = 0;
        } else if (text.toString().equals("pandora")) {
            partition = 1;
        } else {
            partition = 2;
        }
        return partition;
    }
}

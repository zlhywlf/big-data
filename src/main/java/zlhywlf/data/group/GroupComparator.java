package zlhywlf.data.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author zlhywlf
 */
public class GroupComparator extends WritableComparator {
    public GroupComparator() {
        super(GroupBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        final GroupBean o1 = (GroupBean) a;
        final GroupBean o2 = (GroupBean) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}

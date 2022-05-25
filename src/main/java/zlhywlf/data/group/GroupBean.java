package zlhywlf.data.group;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zlhywlf
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class GroupBean implements WritableComparable<GroupBean> {
    private String orderId;
    private Double price;

    @Override
    public int compareTo(GroupBean o) {
        int res = this.orderId.compareTo(o.getOrderId());
        if (res == 0) {
            res = -this.price.compareTo(o.getPrice());
        }
        return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }
}

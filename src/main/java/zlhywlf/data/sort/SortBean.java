package zlhywlf.data.sort;

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
public class SortBean implements WritableComparable<SortBean> {
    private Long selfDuration;
    private Long thirdPartDuration;
    private String deviceId;
    private Long sumDuration;

    @Override
    public int compareTo(SortBean o) {
        System.out.println("compareTo 方法执行了。。。");
        //指定按照bean对象的总时长字段的值进行比较
        return o.sumDuration.compareTo(this.sumDuration);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeUTF(deviceId);
        dataOutput.writeLong(sumDuration);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.selfDuration = dataInput.readLong();
        this.thirdPartDuration = dataInput.readLong();
        this.deviceId = dataInput.readUTF();
        this.sumDuration = dataInput.readLong();
    }
}

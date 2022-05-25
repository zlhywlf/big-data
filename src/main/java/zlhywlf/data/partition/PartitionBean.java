package zlhywlf.data.partition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zlhywlf
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PartitionBean implements Writable {
    private String id;
    private String deviceId;
    private String appKey;
    private String ip;
    private Long selfDuration;
    private Long thirdPartDuration;
    private String status;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(deviceId);
        dataOutput.writeUTF(appKey);
        dataOutput.writeUTF(ip);
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeUTF(status);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.deviceId = dataInput.readUTF();
        this.appKey = dataInput.readUTF();
        this.ip = dataInput.readUTF();
        this.selfDuration = dataInput.readLong();
        this.thirdPartDuration = dataInput.readLong();
        this.status = dataInput.readUTF();
    }
}

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AggregateBackupOperation extends Operation implements BackupOperation {
    private String objectId;
    private List<String> data;

    public AggregateBackupOperation() {
    }

    public AggregateBackupOperation(String objectId, List<String> data) {
        this.objectId = objectId;
        this.data = data;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(objectId);
        out.writeUTFArray(data.toArray(new String[1]));
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        objectId = in.readUTF();
        data = Arrays.asList(in.readUTFArray());
    }

    public void run() throws Exception {
        AggregatorService service = getService();
        System.out.println("Executing backup " + objectId + ".aggregate() on: " + getNodeEngine().getThisAddress());

        Container container = service.containers[getPartitionId()];
        container.aggregate(objectId, data);
    }
}

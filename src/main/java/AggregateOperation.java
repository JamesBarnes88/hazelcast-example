import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AggregateOperation extends Operation implements BackupAwareOperation {

    private String objectId;
    private List<String> data;
    private List<String> returnValue;

    public AggregateOperation() {
    }

    public AggregateOperation(String objectId, List<String> data) {
        this.objectId = objectId;
        this.data = data;
    }

    public void run() throws Exception {
        AggregatorService service = getService();
        System.out.println("Executing " + objectId + ".aggregate() on: " + getNodeEngine().getThisAddress());

        Container container = service.containers[getPartitionId()];
        returnValue = container.aggregate(objectId, data);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    public boolean shouldBackup() {
        return true;
    }

    public int getSyncBackupCount() {
        return 1;
    }

    public int getAsyncBackupCount() {
        return 0;
    }

    public Operation getBackupOperation() {
        return new AggregateBackupOperation(objectId, data);
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
}

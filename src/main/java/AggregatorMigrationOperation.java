import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatorMigrationOperation extends Operation{
    private Map<String, List<String>> migrationData;

    public AggregatorMigrationOperation() {
    }

    public AggregatorMigrationOperation(Map<String, List<String>> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, List<String>> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTFArray(entry.getValue().toArray(new String[1]));
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        migrationData = new HashMap<String, List<String>>();
        for (int i = 0; i < size; i++) {
            migrationData.put(in.readUTF(), Arrays.asList(in.readUTFArray()));
        }
    }

    public void run() throws Exception {
        AggregatorService service = getService();
        Container container = service.containers[getPartitionId()];
        container.applyMigrationData(migrationData);
    }
}

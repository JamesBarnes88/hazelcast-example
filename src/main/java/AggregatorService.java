import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.*;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AggregatorService implements ManagedService, RemoteService, MigrationAwareService {
    public static final String NAME = "AggregatorService";

    Container[] containers;
    private NodeEngine nodeEngine;

    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        containers = new Container[nodeEngine.getPartitionService().getPartitionCount()];
        for(int i = 0; i < containers.length; i++) {
            containers[i] = new Container();
        }
    }

    public void reset() {

    }

    public void shutdown(boolean terminate) {

    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Container container = containers[event.getPartitionId()];
        Map<String, List<String>> migrationData = container.getMigrationData();
        if (migrationData.isEmpty()) {
            return null;
        }

        return new AggregatorMigrationOperation(migrationData);
    }

    public void beforeMigration(PartitionMigrationEvent event) {
        // no-op
    }

    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int newReplicaIndex = event.getNewReplicaIndex();
            if (newReplicaIndex == -1 || newReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int currentReplicaIndex = event.getCurrentReplicaIndex();
            if (currentReplicaIndex == -1 || currentReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    public void clearPartitionReplica(int partitionId) {
        Container container = containers[partitionId];
        container.clear();
    }

    public DistributedObject createDistributedObject(String objectName) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectName);
        Container container = containers[partitionId];
        container.init(objectName);
        return new AggregatorProxy(objectName, nodeEngine, this);
    }

    public void destroyDistributedObject(String objectName) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectName);
        Container container = containers[partitionId];
        container.destroy(objectName);
    }
}

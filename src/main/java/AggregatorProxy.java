import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.List;
import java.util.concurrent.Future;

public class AggregatorProxy implements Aggregator {

    private final String objectId;
    private final NodeEngine nodeEngine;

    public AggregatorProxy(String objectId, NodeEngine nodeEngine, AggregatorService service) {
        this.nodeEngine = nodeEngine;
        this.objectId = objectId;
    }

    public List<String> aggregate(List<String> data) {
        AggregateOperation operation = new AggregateOperation(objectId, data);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(objectId);
        InvocationBuilder builder = nodeEngine.getOperationService()
                .createInvocationBuilder(AggregatorService.NAME, operation, partitionId);
        try {
            Future future = builder.invoke();
            return (List<String>) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public String getPartitionKey() {
        return null;
    }

    public String getName() {
        return objectId;
    }

    public String getServiceName() {
        return AggregatorService.NAME;
    }

    public void destroy() {

    }
}

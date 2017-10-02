import com.hazelcast.core.DistributedObject;

import java.util.List;

public interface Aggregator extends DistributedObject {

    List<String> aggregate(List<String> data);
}

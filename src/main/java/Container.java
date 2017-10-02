import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Container {

    private final ConcurrentMap<String, List<String>> aggregationMap = new ConcurrentHashMap<String, List<String>>();

    List<String> aggregate(String id, List<String> data) {
        List<String> aggregation = aggregationMap.get(id);
        if (aggregation == null) {
            aggregation = new ArrayList<String>();
        }

        aggregation.addAll(data);
        aggregationMap.put(id, aggregation);

        return aggregation;
    }

    void clear() {
        aggregationMap.clear();
    }

    void applyMigrationData(Map<String, List<String>> migrationData) {
        aggregationMap.putAll(migrationData);
    }

    Map<String, List<String>> getMigrationData() {
        return new HashMap<String, List<String>>(aggregationMap);
    }

    void init(String objectName) {
        aggregationMap.put(objectName, new ArrayList<String>());
    }

    void destroy(String objectName) {
        aggregationMap.remove(objectName);
    }
}

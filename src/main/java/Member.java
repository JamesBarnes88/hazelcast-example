import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.ArrayList;
import java.util.List;

public class Member {
    public static void main(String[] args) {
        final HazelcastInstance[] instances = new HazelcastInstance[4];
        for (int i = 0; i < instances.length; i++) {
            HazelcastInstance instance = Hazelcast.newHazelcastInstance();
            instances[i] = instance;
        }

        List<String> data = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            data.add("Hello world this is 0");
        }

        Aggregator aggregator = instances[0].getDistributedObject(AggregatorService.NAME, "myAggregator");
        List<String> rdata = aggregator.aggregate(data);
        printList(rdata);

        List<String> data1 = new ArrayList<String>();
        for (int i = 0; i < 20; i++) {
            data1.add("Hello world this is 1");
        }

        aggregator = instances[1].getDistributedObject(AggregatorService.NAME, "myAggregator");
        List<String> rdata1 = aggregator.aggregate(data1);
        printList(rdata1);

        Hazelcast.shutdownAll();
    }

    public static void printList(List<String> data) {
        for (String line : data) {
            System.out.println(line);
        }
    }
}

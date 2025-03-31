package hdfs_map_reduce;

import java.util.List;

public class Interfaces {

    public interface MapContext {
        void write(String key, String value);
    }

    public interface Mapper {
        void map(String line, MapContext context);
    }

    public interface ReduceContext {
        void write(String key, String aggregatedValue);
    }

    public interface Reducer {
        void reduce(String key, List<String> values, ReduceContext context);
    }
}

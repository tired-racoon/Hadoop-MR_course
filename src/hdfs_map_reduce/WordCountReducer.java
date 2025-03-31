package hdfs_map_reduce;

import java.util.List;

public class WordCountReducer implements Interfaces.Reducer {
    @Override
    public void reduce(String key, List<String> values, Interfaces.ReduceContext context) {
        int sum = 0;
        for (String val : values) {
            sum += Integer.parseInt(val);
        }
        context.write(key, String.valueOf(sum));
    }
}

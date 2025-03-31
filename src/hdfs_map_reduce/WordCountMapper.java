package hdfs_map_reduce;

public class WordCountMapper implements Interfaces.Mapper {
    @Override
    public void map(String line, Interfaces.MapContext context) {
        String[] words = line.split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(word, "1");
            }
        }
    }
}

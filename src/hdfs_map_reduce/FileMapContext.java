package hdfs_map_reduce;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;


public class FileMapContext implements Interfaces.MapContext {
    private final BufferedWriter writer;

    public FileMapContext(String filePath) throws IOException {
        writer = new BufferedWriter(new FileWriter(filePath, true));
    }

    @Override
    public synchronized void write(String key, String value) {
        try {
            writer.write(key + "\t" + value);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        writer.close();
    }
}

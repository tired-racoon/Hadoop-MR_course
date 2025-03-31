package hdfs_map_reduce;

import java.io.*;

public class MapperThread extends Thread {
    private final File inputFile;
    private final Interfaces.Mapper mapper;
    private final int mapperId;
    private final int numReducers;
    private final String intermediateDir;

    public MapperThread(File inputFile, Interfaces.Mapper mapper, int mapperId, int numReducers, String intermediateDir) {
        this.inputFile = inputFile;
        this.mapper = mapper;
        this.mapperId = mapperId;
        this.numReducers = numReducers;
        this.intermediateDir = intermediateDir;
    }

    @Override
    public void run() {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {

            // Создаём файлы под каждую партицию редюсера
            FileMapContext[] contexts = new FileMapContext[numReducers];
            for (int i = 0; i < numReducers; i++) {
                String fileName = intermediateDir + File.separator + "mapper" + mapperId + "_to_reducer" + i + ".txt";
                contexts[i] = new FileMapContext(fileName);
            }

            String line;
            while ((line = br.readLine()) != null) {
                mapper.map(line, new Interfaces.MapContext() {
                    @Override
                    public void write(String key, String value) {
                        int partition = Math.abs(key.hashCode()) % numReducers;
                        contexts[partition].write(key, value);
                    }
                });
            }
            for (FileMapContext ctx : contexts) {
                ctx.close();
            }
            File doneFile = new File(intermediateDir + File.separator + "mapper" + mapperId + ".done");
            doneFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

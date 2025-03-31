package hdfs_map_reduce;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class  Main  {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Command: <input_path> <output_path> <num_reducers> <mapper> <reducer>");
            System.exit(1);
        }

        String input_path = args[0];
        String output_path = args[1];
        int numReducers = Integer.parseInt(args[2]);
        String mapperClassName = args[3];
        String reducerClassName = args[4];

        Interfaces.Mapper mapper = (Interfaces.Mapper) Class.forName(mapperClassName).getDeclaredConstructor().newInstance();
        Interfaces.Reducer reducer = (Interfaces.Reducer) Class.forName(reducerClassName).getDeclaredConstructor().newInstance();

        // Директория обмена корзинами между маппером и редюсером
        String temp_dir = output_path + File.separator + "temporary";
        new File(temp_dir).mkdirs();

        // Получаем список входных файлов
        File dir = new File(input_path);
        File[] inputFiles = dir.listFiles();
        if (inputFiles == null) {
            System.err.println("Input dir does not exist or is not a directory or it's empty");
            System.exit(1);
        }

        List<Thread> mapperThreads = new ArrayList<>();
        for (int i = 0; i < inputFiles.length; i++) {
            MapperThread mt = new MapperThread(inputFiles[i], mapper, i, numReducers, temp_dir);
            mapperThreads.add(mt);
            mt.start();
        }

        List<Thread> reducerThreads = new ArrayList<>();
        for (int i = 0; i < numReducers; i++) {
            ReducerThread rt = new ReducerThread(i, reducer, inputFiles.length, temp_dir, output_path);
            reducerThreads.add(rt);
            rt.start();
        }

        for (Thread t : mapperThreads) {
            t.join();
        }
        for (Thread t : reducerThreads) {
            t.join();
        }

        System.out.println("Success");
    }
}

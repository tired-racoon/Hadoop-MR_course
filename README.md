# Hadoop-MR_course
Домашнее задание по курсу Hadoop и MapReduce

Общий шаблон запуска:
```"Command: <input_path> <output_path> <num_reducers> <mapper> <reducer>"```

В нашем случае ```java <input_path> <output_path> <num_reducers> hdfs_map_reduce.WordCountMapper hdfs_map_reduce.WordCountReducer```

Interfaces.java содержит шаблоны MapContext, Mapper, ReduceContext и Reducer, которые релизуются непосредственно в соответсвующих задаче подсчета слов java-классах

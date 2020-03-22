# 第7章 MapReduce

<!-- omit in toc -->
## 目录

- [7.5 MapReduce 编程实践](#75-mapreduce-编程实践)
  - [7.5.1 任务要求](#751-任务要求)
  - [7.5.2 编写 `Map` 处理逻辑](#752-编写-map-处理逻辑)
  - [7.5.3 编写 `Reduce` 处理逻辑](#753-编写-reduce-处理逻辑)
  - [7.5.4 编写 `main()` 方法](#754-编写-main-方法)
  - [7.5.5 编译打包代码以及运行程序](#755-编译打包代码以及运行程序)

## 7.5 MapReduce 编程实践

&emsp;&emsp;在第2章，我们已经介绍了如何在单台机器上搭建伪分布式 Hadoop 环境，并介绍了如何利用 Hadoop 自带的实例程序来分析数据。现在介绍如何编写基本的 MapReduce 程序帮助自己实现数据分析。下面首先给出基本任务要求，然后阐述如何编写 MapReduce 程序来实现任务要求。

&emsp;&emsp;课本示例用于 Apache Hadoop v2.7.3 ，但以下运行效果来自 Apache Hadoop v2.7.1 。

### 7.5.1 任务要求

&emsp;&emsp;在第2章中，我们运行 Hadoop 自带的实例程序统计了 `input/` 文件夹下所有文件中每个单词出现的次数；在第7.3节介绍了介绍了用 MapReduce 程序实现单词出现次数统计的基本思路和具体执行过程。下面介绍如何编写具体实现代码以及如何运行程序。

&emsp;&emsp;首先在本地创建2个文件：

```text
root@Huawei-ECS:~/文档/Hadoop # tree
/root/文档/Hadoop
└── input
   ├── A.txt
   └── B.txt

1 directory, 2 files

root@Huawei-ECS:~/文档/Hadoop # cat input/A.txt
China is my motherland
I love China

root@Huawei-ECS:~/文档/Hadoop # cat input/B.txt
I am from China
```

&emsp;&emsp;假设 HDFS 中已经创建好了一个 `intput/` 文件夹，现在把2个文件上传到 HDFS 的 `input/` 文件夹下（注意，上传之前，请清空 `input/` 文件夹中原有的文件）。

```text
root@Huawei-ECS:~/文档/Hadoop # hdfs dfs -put input/
root@Huawei-ECS:~/文档/Hadoop # hdfs dfs -ls -R ./
drwxr-xr-x   - root supergroup          0 2020-03-22 20:03 input
-rw-r--r--   1 root supergroup         36 2020-03-22 20:03 input/A.txt
-rw-r--r--   1 root supergroup         16 2020-03-22 20:03 input/B.txt
```

&emsp;&emsp;现在的目标是统计 `input/` 文件夹下所有文件中每个单词的出现次数。在编写完整程序之前，我们需要创建 Eclipse 项目及相应文件，获得以下目录结构：

```text
root@Huawei-ECS:~/文档/Hadoop/WordCount # tree
/root/文档/Hadoop/WordCount
├── bin
│   ├── app
│   │   ├── WordCount$Status.class
│   │   └── WordCount.class
│   └── mapreduce
│       ├── IntSumReducer.class
│       └── TokenizerMapper.class
└── src
    ├── app
    │   └── WordCount.java
    └── mapreduce
        ├── IntSumReducer.java
        └── TokenizerMapper.java

6 directories, 7 files
```

### 7.5.2 编写 `Map` 处理逻辑

```java
// TokenizerMapper.java
package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 令牌化Map任务类
 *
 * @author Dragon1573
 */
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    /** 常量可写入整数值 */
    private static final IntWritable one = new IntWritable(1);
    /** 文本内容 */
    private Text word = new Text();

    public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            this.word.set(itr.nextToken());
            context.write(this.word, one);
        }
    }
}
```

### 7.5.3 编写 `Reduce` 处理逻辑

```java
package mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 整数求和Reduce任务类
 *
 * @author Dragon1573
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        IntWritable val;
        for (Iterator<IntWritable> i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
            val = (IntWritable) i$.next();
        }
        this.result.set(sum);
        context.write(key, result);
    }
}
```

### 7.5.4 编写 `main()` 方法

```java
// WordCount.java
package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mapreduce.*;

/**
 * 词频统计主程序
 *
 * @author Dragon1573
 */
public class WordCount {
    /**
     * 退出码常量
     */
    private static class Status {
        /** 正常退出 */
        public static final int SUCCESS = 0;
        /** 任务未完成 */
        public static final int FAILED = 1;
        /** 参数语法错误 */
        public static final int ILLEGAL_ARGUMENTS = 2;
    }

    public static void main(String[] args) throws Exception {
        // 创建Hadoop配置
        Configuration conf = new Configuration();
        // 解析参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            // 参数错误
            System.err.println("Usage: wordcount <input_1> [<input_2> ...] <output>");
            System.exit(Status.ILLEGAL_ARGUMENTS);
        }

        // 创建MapReduce任务
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? Status.SUCCESS : Status.FAILED);
    }
}
```

### 7.5.5 编译打包代码以及运行程序

&emsp;&emsp;由于 Visual Studio Code 会对代码进行实时编译，所以我们不需要再对源代码执行编译命令了。

```text
root@Huawei-ECS:~/文档/Hadoop/WordCount # tree bin
bin
├── app
│   ├── WordCount$Status.class
│   └── WordCount.class
└── mapreduce
    ├── IntSumReducer.class
    └── TokenizerMapper.class

2 directories, 4 files
```

&emsp;&emsp;直接将编译好的二进制字节码打包为可执行 `*.jar` 文件。

```text
root@Huawei-ECS:~/文档/Hadoop/WordCount # cd bin

root@Huawei-ECS:~/文档/Hadoop/WordCount/bin # jar -cvf WordCount.jar *
已添加清单
正在添加: app/(输入 = 0) (输出 = 0)(存储了 0%)
正在添加: app/WordCount.class(输入 = 2151) (输出 = 1160)(压缩了 46%)
正在添加: app/WordCount$Status.class(输入 = 455) (输出 = 317)(压缩了 30%)
正在添加: mapreduce/(输入 = 0) (输出 = 0)(存储了 0%)
正在添加: mapreduce/TokenizerMapper.class(输入 = 2210) (输出 = 876)(压缩了 60%)
正在添加: mapreduce/IntSumReducer.class(输入 = 2409) (输出 = 919)(压缩了 61%)

root@Huawei-ECS:~/文档/Hadoop/WordCount/bin # ll WordCount.jar
-rw-r--r-- 1 root root 4.3K 3月  22 19:42 WordCount.jar
```

&emsp;&emsp;由于 Hadoop 服务已经提前启动并常开，所以不需要再通过 `start-dfs.sh` 启动 Hadoop 服务。

```text
root@Huawei-ECS：~/文档/Hadoop/WordCount/bin # jps
12226 DataNode
12069 NameNode
12422 SecondaryNameNode
21878 XMLServerLauncher
25902 Jps
21134 org.eclipse.equinox.launcher_1.5.700.v20200207-2156.jar

root@Huawei-ECS:~/文档/Hadoop/WordCount/bin # hadoop jar WordCount.jar app.WordCount input output 2>&1 | grep -v "INFO"
20/03/22 21:21:37 WARN io.ReadaheadPool: Failed readahead on ifile
EBADF: Bad file descriptor
    at org.apache.hadoop.io.nativeio.NativeIO$POSIX.posix_fadvise(Native Method)
    at org.apache.hadoop.io.nativeio.NativeIO$POSIX.posixFadviseIfPossible(NativeIO.java:267)
    at org.apache.hadoop.io.nativeio.NativeIO$POSIX$CacheManipulator.posixFadviseIfPossible(NativeIO.java:146)
    at org.apache.hadoop.io.ReadaheadPool$ReadaheadRequestImpl.run(ReadaheadPool.java:206)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:748)
20/03/22 21:21:37 WARN io.ReadaheadPool: Failed readahead on ifile
EBADF: Bad file descriptor
    at org.apache.hadoop.io.nativeio.NativeIO$POSIX.posix_fadvise(Native Method)
    at org.apache.hadoop.io.nativeio.NativeIO$POSIX.posixFadviseIfPossible(NativeIO.java:267)
    at org.apache.hadoop.io.nativeio.NativeIO$POSIX$CacheManipulator.posixFadviseIfPossible(NativeIO.java:146)
    at org.apache.hadoop.io.ReadaheadPool$ReadaheadRequestImpl.run(ReadaheadPool.java:206)
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    at java.lang.Thread.run(Thread.java:748)
    File System Counters
        FILE: Number of bytes read=14717
        FILE: Number of bytes written=842212
        FILE: Number of read operations=0
        FILE: Number of large read operations=0
        FILE: Number of write operations=0
        HDFS: Number of bytes read=140
        HDFS: Number of bytes written=54
        HDFS: Number of read operations=22
        HDFS: Number of large read operations=0
        HDFS: Number of write operations=5
    Map-Reduce Framework
        Map input records=3
        Map output records=11
        Map output bytes=96
        Map output materialized bytes=118
        Input split bytes=216
        Combine input records=11
        Combine output records=10
        Reduce input groups=8
        Reduce shuffle bytes=118
        Reduce input records=10
        Reduce output records=8
        Spilled Records=20
        Shuffled Maps =2
        Failed Shuffles=0
        Merged Map outputs=2
        GC time elapsed (ms)=8
        Total committed heap usage (bytes)=957349888
    Shuffle Errors
        BAD_ID=0
        CONNECTION=0
        IO_ERROR=0
        WRONG_LENGTH=0
        WRONG_MAP=0
        WRONG_REDUCE=0
    File Input Format Counters
        Bytes Read=52
    File Output Format Counters
        Bytes Written=54

        Map output bytes=96

root@Huawei-ECS:~/文档/Hadoop/WordCount/bin # hdfs dfs -cat "output/*"

China   3
I       2
am      1
from    1
is      1
love    1
motherland      1
my      1
```

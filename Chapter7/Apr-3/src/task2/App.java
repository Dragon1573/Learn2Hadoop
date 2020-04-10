package task2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {
    public static void main(final String[] args)
            throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length != 2) {
            System.err.println("用法：task2.App <in> <out>");
            throw new IllegalArgumentException("程序必须有且只有2个参数！");
        }

        final Configuration conf = new Configuration();
        final Path outputPath = new Path(args[1]);
        FileSystem.get(conf).delete(outputPath, true);
        final Job job = Job.getInstance(conf, "Secondary Sort");
        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(MyKey.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce任务执行失败！");
        }
        System.out.println("MapReduce任务执行成功！");
    }
}

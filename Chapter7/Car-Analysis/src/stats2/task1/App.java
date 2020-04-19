// App.java
package stats2.task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * (1) 统计买车的男女性别比例以及男女对车的颜色的选择
 *
 * @author Dragon1573
 */
public class App {
    private static final Path IN_PATH = new Path("input");
    private static final Path OUT_PATH = new Path("output/task2-1");

    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();
        FileSystem.get(conf).delete(OUT_PATH, true);

        final Job job = Job.getInstance(conf, "Analysis 2-1");
        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job, IN_PATH);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUT_PATH);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("MapReduce Failed!");
        }
        System.out.println("MapReduce Succeeded!");
    }
}

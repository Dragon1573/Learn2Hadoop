package stats1.task1;

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
 * (1) 通过统计乘用车辆和商用车辆的数量和销售额分布
 *
 * @author Dragon1573
 * @since 2020/04/17
 */
public class App {
    private static final Path INPUT_PATH = new Path("/user/root/input");
    private static final Path OUTPUT_PATH = new Path("/user/root/output/task1-1");

    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();
        FileSystem.get(conf).delete(OUTPUT_PATH, true);

        final Job job = Job.getInstance(conf, "Analysis 1-1");
        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job, INPUT_PATH);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce Job Failed!");
        }
        System.out.println("MapReduce Job Finished!");
    }
}

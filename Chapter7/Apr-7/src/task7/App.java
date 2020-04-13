package task7;

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
 * 统计不同车型的用户的年龄和性别
 *
 * @author Dragon1573
 */
public class App {
    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length != 2) {
            System.err.println("Usage: task7.App <in> <out>");
            throw new IllegalArgumentException("ONLY 2 arguments are required!");
        }

        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "Period Gender Counts");
        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        final Path outputPath = new Path(args[1]);
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce execution failed!");
        }
    }
}

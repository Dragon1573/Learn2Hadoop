package task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 提取城市及其机场对应关系
 *
 * @author Dragon1573
 */
public class App {
    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length != 3) {
            System.err.println("Usage: task1.App <all_city> <some_city> <out>");
            throw new IllegalArgumentException("ONLY 3 arguemnts are required!");
        }

        final Configuration conf = new Configuration();
        FileSystem.get(conf).delete(new Path(args[2]), true);
        final Job job = Job.getInstance(conf, "Airport in Cities");
        job.setJarByClass(App.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AllCity.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SomeCity.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce execution failed!");
        }
    }
}

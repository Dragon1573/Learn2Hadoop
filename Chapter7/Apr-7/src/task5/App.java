package task5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 统计各月销售占比
 */
public class App {
    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args == null || args.length != 2) {
            System.err.println("Usage: task5.App <in> <out>");
            throw new IllegalArgumentException("ONLY 2 arguments are required!");
        }

        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "Sales percentage per Month");
        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce execution failed!");
        }
    }
}

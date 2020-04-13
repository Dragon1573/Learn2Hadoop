package task3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 统计发动机型号和油耗的对应关系
 *
 * @author Dragon1573
 */
public class App {
    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.err.println("Usage: task3.App <in> <out>");
            throw new IllegalArgumentException("ONLY 2 arguments are required!");
        }

        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "Type and Fuel");
        job.setJarByClass(App.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(false)) {
            throw new RuntimeException("MapReduce execution failed!");
        }
    }
}

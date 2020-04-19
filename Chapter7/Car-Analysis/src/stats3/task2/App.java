// App.java
package stats3.task2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * (2) 通过不同类型（品牌）车销售情况，来统计发动机型号和燃料种类
 *
 * @author Dragon1573
 */
public class App {
    private static final Path IN_PATH = new Path("input");
    private static final Path OUT_PATH = new Path("output/task3-2");

    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();
        FileSystem.get(conf).delete(OUT_PATH, true);

        final Job job = Job.getInstance(conf, "Analysis 3-2");
        job.setJarByClass(App.class);
        FileInputFormat.addInputPath(job, IN_PATH);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUT_PATH);

        if (!job.waitForCompletion(false)) {
            System.out.println("[ERROR] MapReduce Failed!");
        }
        System.out.println("[SUCCESS] MapReduce Succeeded!");
    }
}

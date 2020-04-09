package task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        if (columns != null && columns.length == 39) {
            final String gender = columns[38];
            if (gender != null && gender.contains("性")) {
                context.write(new Text(gender), ONE);
            }
        }
    }
}

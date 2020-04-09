package task6;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text mapKey = new Text();

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        if (columns != null && columns.length == 39) {
            mapKey.set(columns[10]);
            context.write(mapKey, ONE);
        }
    }
}

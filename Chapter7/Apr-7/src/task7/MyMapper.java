package task7;

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
        final String[] columns = value.toString().trim().split(",");
        if (columns != null && columns.length == 39 && columns[8] != null && columns[37] != null
                && columns[38] != null) {
            try {
                final int age = Integer.parseInt(columns[4]) - Integer.parseInt(columns[37]);
                final int rangeLeft = age / 10 * 10;
                final int rangeRight = rangeLeft + 10;
                final Text mapKey = new Text(columns[8] + "\t" + rangeLeft + "-" + rangeRight + "\t" + columns[38]);
                context.write(mapKey, ONE);
            } catch (Exception ignored) {
            }
        }
    }
}

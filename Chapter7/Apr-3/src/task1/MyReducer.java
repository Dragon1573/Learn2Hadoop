package task1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Dragon1573
 */
public class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        boolean hasAirport = false;
        for (final Text v : values) {
            if (v.toString().contains("机场")) {
                hasAirport = true;
                break;
            }
        }
        if (!hasAirport) {
            context.write(key, null);
        }
    }
}

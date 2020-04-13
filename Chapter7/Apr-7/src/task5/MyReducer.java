package task5;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Dragon1573
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    private final DoubleWritable percentage = new DoubleWritable();
    private final Map<String, Integer> map = new HashMap<>();
    private int total = 0;

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        int counter = 0;
        for (final IntWritable v : values) {
            counter += v.get();
        }
        total += counter;
        map.put(key.toString(), counter);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (final String key : map.keySet()) {
            final int value = map.get(key);
            percentage.set((double) value / (double) total);
            context.write(new Text(key), percentage);
        }
    }
}

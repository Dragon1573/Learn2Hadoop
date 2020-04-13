package task2;

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
    private final Map<String, Integer> map = new HashMap<>();
    double total = 0;

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        total += sum;
        map.put(key.toString(), sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (final String key : map.keySet()) {
            final int value = map.get(key);
            final double percentage = value / total;
            context.write(new Text(key), new DoubleWritable(percentage));
        }
    }
}

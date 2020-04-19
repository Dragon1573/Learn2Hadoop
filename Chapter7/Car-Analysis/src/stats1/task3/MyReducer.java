// MyReducer.java
package stats1.task3;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Dragon1573
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Map<Text, Integer> map = new HashMap<>();
    private int total = 0;

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (final IntWritable v : values) {
            sum += v.get();
        }
        map.put(key, sum);
        total += sum;
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        for (final Text k : map.keySet()) {
            // Convert into percentages.
            final DecimalFormat format = new DecimalFormat("#0.00%");
            final String outValue = format.format(map.get(k).doubleValue() / total);

            context.write(k, new Text(outValue));
        }
    }
}

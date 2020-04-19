// MyReducer.java
package stats2.task1;

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
    private int total = 0;
    private Map<String, Integer> map = new HashMap<>();

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (final IntWritable v : values) {
            sum += v.get();
        }
        map.put(key.toString(), sum);
        total += sum;
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        for (final String k : map.keySet()) {
            final DecimalFormat format = new DecimalFormat("#0.00%");
            final String outValue = format.format(map.get(k).doubleValue() / total);
            context.write(new Text(k), new Text(outValue));
        }
    }
}

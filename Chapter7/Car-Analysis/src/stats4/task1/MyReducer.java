// MyReducer.java
package stats4.task1;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Dragon1573
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
    public class Struct {
        public String string;
        public int count;

        public Struct(final String string, final int count) {
            this.count = count;
            this.string = string;
        }
    }

    private final List<Struct> list = new LinkedList<>();

    @Override
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (final IntWritable v : values) {
            sum += v.get();
        }
        list.add(new Struct(key.toString(), sum));
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        int lastValue = list.get(0).count;
        for (final Struct struct : list) {
            final double diff = (struct.count - lastValue) / (double) lastValue;
            final DecimalFormat format = new DecimalFormat("#0.00%");
            final String outValue = String.format("%4d\t%7s", struct.count, format.format(diff));
            context.write(new Text(struct.string), new Text(outValue));
            lastValue = struct.count;
        }
    }
}

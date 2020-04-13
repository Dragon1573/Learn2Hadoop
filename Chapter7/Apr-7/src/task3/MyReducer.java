package task3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Dragon1573
 */
public class MyReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        Set<String> set = new HashSet<>();
        for (final Text text : values) {
            set.add(text.toString());
        }
        for (final String string : set) {
            context.write(key, new Text(string));
        }
    }
}

package task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class SomeCity extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split("\\t");
        context.write(new Text(columns[0]), new Text(columns[1]));
    }
}

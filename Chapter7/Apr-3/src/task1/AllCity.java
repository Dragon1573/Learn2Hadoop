package task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class AllCity extends Mapper<Object, Text, Text, Text> {
    private static final Text NULL_STRING = new Text("");

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        context.write(value, NULL_STRING);
    }
}

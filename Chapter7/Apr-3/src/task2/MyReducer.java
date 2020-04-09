package task2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<MyKey, NullWritable, MyKey, NullWritable> {
    @Override
    protected void reduce(final MyKey key, final Iterable<NullWritable> values, final Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}

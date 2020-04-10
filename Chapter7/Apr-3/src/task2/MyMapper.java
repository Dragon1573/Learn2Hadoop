package task2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<LongWritable, Text, MyKey, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] columns = value.toString().split("\t");
        final MyKey myKey = new MyKey(Integer.parseInt(columns[0]), Integer.parseInt(columns[1]));
        context.write(myKey, NullWritable.get());
    }
}

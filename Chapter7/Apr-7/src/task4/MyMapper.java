package task4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        if (columns != null && columns.length == 39) {
            final String month = columns[1];
            final String brand = columns[7];
            final int sales = Integer.parseInt(columns[11]);
            context.write(new Text(month + "月-" + brand), new IntWritable(sales));
        }
    }
}

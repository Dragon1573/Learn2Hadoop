// MyMapper.java
package stats2.task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        if (columns != null && columns.length == 39) {
            final String ownerShip = columns[9];
            final String model = columns[5];
            final String type = columns[8];
            if (ownerShip != null && model != null && type != null) {
                if (!"".equals(ownerShip) && !"".equals(model) && !"".equals(type)) {
                    final String outStr = String.format("%8s %16s %16s", ownerShip, model, type);
                    final Text outKey = new Text(outStr);
                    context.write(outKey, ONE);
                }
            }
        }
    }
}

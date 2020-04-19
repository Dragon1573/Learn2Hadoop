// MyMapper.java
package stats1.task3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable();

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        if (columns != null && columns.length == 39) {
            final String province = columns[0];
            final String year = columns[4];
            final String month = columns[1];
            if (province != null && year != null && month != null) {
                if ("安徽省".equals(province) && "2014".equals(year) && "4".equals(month)) {
                    final Text outKey = new Text(province + "\t" + year + "年\t" + month + "月");
                    context.write(outKey, ONE);
                }
            }
        }
    }
}

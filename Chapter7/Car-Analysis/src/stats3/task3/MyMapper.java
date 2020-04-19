// MyMapper.java
package stats3.task3;

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
        if (columns.length == 39) {
            // 品牌
            final String tradeMark = columns[7];
            if (!"".equals(tradeMark) && tradeMark.matches("\\D+")) {
                context.write(new Text(String.format("%6s", tradeMark)), ONE);
            }
        }
    }
}

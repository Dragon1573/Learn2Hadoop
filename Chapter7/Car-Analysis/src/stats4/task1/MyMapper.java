// MyMapper.java
package stats4.task1;

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
            // 年份
            final String year = columns[4];
            // 月份
            final String month = columns[1];
            if (!"".equals(tradeMark) && !"".equals(year) && !"".equals(month)) {
                if ("五菱".equals(tradeMark) && year.matches("\\d+") && month.matches("\\d+")) {
                    final String outKey = String.format("%2s\t%4s年%2s月", tradeMark, year, month);
                    context.write(new Text(outKey), ONE);
                }
            }
        }
    }
}

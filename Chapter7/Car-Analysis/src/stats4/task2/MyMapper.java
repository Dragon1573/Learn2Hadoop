// MyMapper.java
package stats4.task2;

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
            // 省份
            final String province = columns[0];
            // 品牌
            final String tradeMark = columns[7];
            if ("山西省".equals(province) && tradeMark.contains("五菱")) {
                final String outKey = String.format("%3s\t%6s", province, tradeMark);
                context.write(new Text(outKey), ONE);
            }
        }
    }
}

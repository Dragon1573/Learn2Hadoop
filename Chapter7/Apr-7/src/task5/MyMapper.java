package task5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text month = new Text();
    private final IntWritable counts = new IntWritable();

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        // columns[1]是月份，columns[11]是销量
        if (columns.length > 11 && columns[11] != null && !"".equals(columns[11].trim())) {
            try {
                month.set(columns[1]);
                counts.set(Integer.parseInt(columns[11]));
                context.write(month, counts);
            } catch (Exception ignore) {
            }
        }
    }
}

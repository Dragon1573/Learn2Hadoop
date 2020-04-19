package stats1.task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static Text privateUse = new Text("乘用车辆");
    private static Text publicUse = new Text("商用车辆");
    private static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        if (columns != null && columns.length == 39) {
            if (columns[9] != null && "个人".equals(columns[9])) {
                context.write(privateUse, ONE);
            } else if (columns[9] != null && "单位".equals(columns[9])) {
                context.write(publicUse, ONE);
            }
        }
    }
}

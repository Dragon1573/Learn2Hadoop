package task3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        // columns[7]为车辆品牌，columns[12]为发动机型号，columns[15]为燃油种类
        if (columns.length == 39 && columns != null) {
            final String label = columns[12] + " " + columns[15];
            context.write(new Text(columns[7]), new Text(label));
        }
    }
}

// MyMapper.java
package stats3.task2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, NullWritable> {
    @Override
    protected void map(final Object key, final Text value, final Context context)
            throws IOException, InterruptedException {
        final String[] columns = value.toString().split(",");
        if (columns.length == 39) {
            // 品牌
            final String tradeMark = columns[7];
            // 发动机型号
            final String engineSerial = columns[12];
            // 燃料种类
            final String powerType = columns[15];
            if (!"".equals(tradeMark) && !"".equals(engineSerial) && !"".equals(powerType)) {
                final String outKey = String.format("%8s\t%16s\t%8s", tradeMark, engineSerial, powerType);
                context.write(new Text(outKey), NullWritable.get());
            }
        }
    }
}

// MyMapper.java
package stats3.task1;

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
            final String type = columns[8];
            final String year = columns[4];
            final String monthStr = columns[1];
            if (!"".equals(type) && !"".equals(year) && !"".equals(monthStr)) {
                if (monthStr.matches("\\d+") && year.matches("\\d+")) {
                    final int monthInt = Integer.parseInt(monthStr);
                    final String outStr;
                    switch ((monthInt - 1) / 3) {
                        case 0:
                            outStr = String.format("%6s\t%4s年第一季度", type, year);
                            break;
                        case 1:
                            outStr = String.format("%6s\t%4s年第二季度", type, year);
                            break;
                        case 2:
                            outStr = String.format("%6s\t%4s年第三季度", type, year);
                            break;
                        case 3:
                            outStr = String.format("%6s\t%4s年第四季度", type, year);
                            break;
                        default:
                            outStr = "未知时段";
                            break;
                    }
                    context.write(new Text(outStr), ONE);
                }
            }
        }
    }
}

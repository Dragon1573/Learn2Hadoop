// MyMapper.java
package stats2.task3;

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
            final String salesYear = columns[4];
            final String bornYear = columns[37];
            final String gender = columns[38];
            if (!"".equals(type) && !"".equals(salesYear) && !"".equals(bornYear) && !"".equals(gender)
                    && salesYear.matches("\\d+") && bornYear.matches("\\d+")) {
                int age = Integer.parseInt(salesYear) - Integer.parseInt(bornYear);
                if (age == 0) {
                    ++age;
                }
                final int ageStart = (age / 10 - (age % 10 == 0 ? 1 : 0)) * 10;
                final String period = String.format("%d～%d", (ageStart == 0 ? 0 : ageStart + 1), ageStart + 10);
                final String outStr = String.format("%16s %16s %4s", type, period, gender);
                context.write(new Text(outStr), ONE);
            }
        }
    }
}

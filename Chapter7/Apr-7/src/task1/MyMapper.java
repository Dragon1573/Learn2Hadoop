package task1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Dragon1573
 */
public class MyMapper extends Mapper<Object, Text, Text, Text> {
    private final Text city = new Text();
    private final Text manufacturer = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();
        // 我们输入的文件是CSV格式的，采用逗号分隔
        final String[] columns = line.split(",");
        // 结构化数据共有39列
        if (columns.length == 39 && columns != null) {
            city.set(columns[2]);
            manufacturer.set(columns[6]);
            context.write(city, manufacturer);
        }
    }
}

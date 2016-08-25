import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneItemSetMapper extends Mapper<Object, Text, Text, IntWritable> {
	// output Key
	private Text outKey = new Text();
	// output value
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());

		while (itr.hasMoreTokens()) {
			outKey.set(itr.nextToken());
			context.write(outKey, one);
		}
	}
}

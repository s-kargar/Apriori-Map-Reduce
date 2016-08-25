import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Sina Kargar
 *
 */
public class K_ItemSetReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	public static final int DEFAULT_SUPPORT = 100;
	private IntWritable result = new IntWritable();

	// set support by setup() which is set at driver
	int support;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.support = context.getConfiguration().getInt("support", DEFAULT_SUPPORT);
	}

	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		if (sum >= support) {
			result.set(sum);
			context.write(key, result);
		}

	}
}
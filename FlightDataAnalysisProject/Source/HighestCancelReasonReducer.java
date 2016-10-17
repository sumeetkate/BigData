import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class HighestCancelReasonReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, FloatWritable> {
	String reason = "";
	int max = 0;

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum += ((IntWritable) values.next()).get();
		}
		String s = key.toString();
		if (s.equals(Character.toString('þ'))) {
			output.collect(new Text(
					"The most common reason for flights cancellations"),
					new FloatWritable());
			if (this.reason.equals("A")) {
				output.collect(new Text("Carrier"), new FloatWritable(this.max));
			}
			if (this.reason.equals("B")) {
				output.collect(new Text("Weather"), new FloatWritable(this.max));
			}
			if (this.reason.equals("C")) {
				output.collect(new Text("NAS"), new FloatWritable(this.max));
			}
			if (this.reason.equals("D")) {
				output.collect(new Text("Security"),
						new FloatWritable(this.max));
			}
		}
		if (sum > this.max) {
			this.max = sum;
			this.reason = s;
		}
	}
}
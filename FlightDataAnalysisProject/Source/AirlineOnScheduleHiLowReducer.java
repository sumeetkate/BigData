import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AirlineOnScheduleHiLowReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, FloatWritable> {
	String[][] flights = new String[6][2];
	float average = 0.0F;
	int f = 0;
	int k = 0;

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {
		try {
			float sum = 0.0F;
			int count = 0;
			while (values.hasNext()) {
				sum += ((IntWritable) values.next()).get();
				count++;
			}
			this.average = (sum / count);
			String s = key.toString();
			if (this.f == 0) {
				for (int i = 0; i < 6; i++) {
					this.flights[i][1] = "0";
					this.flights[i][0] = " ";
				}
				this.f = 1;
			}
			if (s.equals(Character.toString('þ'))) {

				for (int i = 0; i < 6; i++) {
					if (i == 0) {
						output.collect(new Text(
								"Highest probabilty of airlines on schedule"),
								new FloatWritable());
					} else if (i == 3) {
						output.collect(new Text(
								"Lowest probabilty of airlines on schedule"),
								new FloatWritable());
					}
					output.collect(
							new Text(this.flights[i][0]),
							new FloatWritable(Float
									.parseFloat(this.flights[i][1])));
				}

			} else if (this.average > Float.parseFloat(this.flights[0][1])) {
				this.flights[2][1] = this.flights[1][1];
				this.flights[2][0] = this.flights[1][0];
				this.flights[1][1] = this.flights[0][1];
				this.flights[1][0] = this.flights[0][0];
				this.flights[0][1] = String.valueOf(this.average);
				this.flights[0][0] = s.replace(" in", "");
			} else if (this.average > Float.parseFloat(this.flights[1][1])) {
				this.flights[2][1] = this.flights[1][1];
				this.flights[2][0] = this.flights[1][0];
				this.flights[1][1] = String.valueOf(this.average);
				this.flights[1][0] = s.replace(" in", "");
			} else if (this.average > Float.parseFloat(this.flights[2][1])) {
				this.flights[2][1] = String.valueOf(this.average);
				this.flights[2][0] = s.replace(" in", "");
			}
			if ((this.average < Float.parseFloat(this.flights[3][1]))
					|| (this.k == 0)) {
				this.flights[5][1] = this.flights[4][1];
				this.flights[5][0] = this.flights[4][0];
				this.flights[4][1] = this.flights[3][1];
				this.flights[4][0] = this.flights[3][0];
				this.flights[3][1] = String.valueOf(this.average);
				this.flights[3][0] = s.replace(" in", "");
				this.k = 1;
			} else if (this.average < Float.parseFloat(this.flights[4][1])) {
				this.flights[5][1] = this.flights[4][1];
				this.flights[5][0] = this.flights[4][0];
				this.flights[4][1] = String.valueOf(this.average);
				this.flights[4][0] = s.replace(" in", "");
			} else if (this.average < Float.parseFloat(this.flights[5][1])) {
				this.flights[5][1] = String.valueOf(this.average);
				this.flights[5][0] = s.replace(" in", "");
			}
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
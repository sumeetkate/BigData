import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AirportTaxiTimeHiLowReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, FloatWritable> {

	Float del = 0f, ttl = 0f;
	String[][] taxi = new String[12][2];
	Text newKey = new Text();
	Float average = 0.0F;
	int f = 0;
	int k = 0;
	int l = 0;

	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, FloatWritable> output, Reporter reporter)
			throws IOException {
		try {

			while (values.hasNext()) {
				del = del + values.next().get();
				ttl = ttl + 1;
			}
			if (this.f == 0) {
				for (int i = 0; i < 12; i++) {
					this.taxi[i][1] = "0";
					this.taxi[i][0] = " ";
				}
				this.f = 1;
			}
			newKey.set(key.toString() + "\t");
			average = del / ttl;

			String s = key.toString();

			if (s.contains(" in")) {
				if (this.average > Float.parseFloat(this.taxi[0][1])) {
					this.taxi[2][1] = this.taxi[1][1];
					this.taxi[2][0] = this.taxi[1][0];
					this.taxi[1][1] = this.taxi[0][1];
					this.taxi[1][0] = this.taxi[0][0];
					this.taxi[0][1] = String.valueOf(this.average);
					this.taxi[0][0] = s.replace(" in", "");
				} else if (this.average > Float.parseFloat(this.taxi[1][1])) {
					this.taxi[2][1] = this.taxi[1][1];
					this.taxi[2][0] = this.taxi[1][0];
					this.taxi[1][1] = String.valueOf(this.average);
					this.taxi[1][0] = s.replace(" in", "");
				} else if (this.average > Float.parseFloat(this.taxi[2][1])) {
					this.taxi[2][1] = String.valueOf(this.average);
					this.taxi[2][0] = s.replace(" in", "");
				}
				if ((this.average < Float.parseFloat(this.taxi[3][1]))
						|| (this.k == 0)) {
					this.taxi[5][1] = this.taxi[4][1];
					this.taxi[5][0] = this.taxi[4][0];
					this.taxi[4][1] = this.taxi[3][1];
					this.taxi[4][0] = this.taxi[3][0];
					this.taxi[3][1] = String.valueOf(this.average);
					this.taxi[3][0] = s.replace(" in", "");
					this.k = 1;
				} else if (this.average < Float.parseFloat(this.taxi[4][1])) {
					this.taxi[5][1] = this.taxi[4][1];
					this.taxi[5][0] = this.taxi[4][0];
					this.taxi[4][1] = String.valueOf(this.average);
					this.taxi[4][0] = s.replace(" in", "");
				} else if (this.average < Float.parseFloat(this.taxi[5][1])) {
					this.taxi[5][1] = String.valueOf(this.average);
					this.taxi[5][0] = s.replace(" in", "");
				}
			} else if (s.contains(" out")) {
				if (this.average > Float.parseFloat(this.taxi[6][1])) {
					this.taxi[8][1] = this.taxi[7][1];
					this.taxi[8][0] = this.taxi[7][0];
					this.taxi[7][1] = this.taxi[6][1];
					this.taxi[7][0] = this.taxi[6][0];
					this.taxi[6][1] = String.valueOf(this.average);
					this.taxi[6][0] = s.replace(" out", "");
				} else if (this.average > Float.parseFloat(this.taxi[7][1])) {
					this.taxi[8][1] = this.taxi[7][1];
					this.taxi[8][0] = this.taxi[7][0];
					this.taxi[7][1] = String.valueOf(this.average);
					this.taxi[7][0] = s.replace(" out", "");
				} else if (this.average > Float.parseFloat(this.taxi[8][1])) {
					this.taxi[8][1] = String.valueOf(this.average);
					this.taxi[8][0] = s.replace(" out", "");
				}
				if ((this.average < Float.parseFloat(this.taxi[9][1]))
						|| (this.l == 0)) {
					this.taxi[11][1] = this.taxi[10][1];
					this.taxi[11][0] = this.taxi[10][0];
					this.taxi[10][1] = this.taxi[9][1];
					this.taxi[10][0] = this.taxi[9][0];
					this.taxi[9][1] = String.valueOf(this.average);
					this.taxi[9][0] = s.replace(" out", "");
					this.l = 1;
				} else if (this.average < Float.parseFloat(this.taxi[10][1])) {
					this.taxi[11][1] = this.taxi[10][1];
					this.taxi[11][0] = this.taxi[10][0];
					this.taxi[10][1] = String.valueOf(this.average);
					this.taxi[10][0] = s.replace(" out", "");
				} else if (this.average < Float.parseFloat(this.taxi[11][1])) {
					this.taxi[11][1] = String.valueOf(this.average);
					this.taxi[11][0] = s.replace(" out", "");
				}
			}
			if (s.equals(Character.toString('þ'))) {
				for (int i = 0; i < 12; i++) {
					if (i == 0) {
						output.collect(
								new Text("Airports with Longest taxi-in"),
								new FloatWritable());
					} else if (i == 3) {
						output.collect(new Text(
								"Airports with Shortest taxi-in"),
								new FloatWritable());
					} else if (i == 6) {
						output.collect(new Text(
								"Airports with Longest taxi-out"),
								new FloatWritable());
					} else if (i == 9) {
						output.collect(new Text(
								"Airports with Shortest taxi-out"),
								new FloatWritable());
					}
					output.collect(
							new Text(this.taxi[i][0]),
							new FloatWritable(Float.parseFloat(this.taxi[i][1])));
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
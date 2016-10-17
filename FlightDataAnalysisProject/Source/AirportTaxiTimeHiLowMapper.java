import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AirportTaxiTimeHiLowMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	private Text mapKey1 = new Text();
	private Text mapKey2 = new Text();
	private IntWritable mapValue1 = new IntWritable();
	private IntWritable mapValue2 = new IntWritable();
	private int i1, i2;
	int k = 0;

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException /* Mapper Function */
	{
		try {

			String[] line = new String[29];
			line = value.toString().split(",");
			mapKey1 = new Text(line[17] + " in");
			mapKey2 = new Text(line[16] + " out");

			if (NumberUtils.isNumber(line[19])) {
				i1 = Integer.valueOf(line[19]);

				if (i1 > 0) {
					mapValue1 = new IntWritable(i1);
				} else {
					mapValue1 = new IntWritable(i1);

				}

				output.collect(mapKey1, mapValue1);

			}

			if (NumberUtils.isNumber(line[20])) {
				i2 = Integer.valueOf(line[20]);

				if (i2 > 0) {
					mapValue2 = new IntWritable(i2);
				} else {
					mapValue2 = new IntWritable(i2);

				}

				output.collect(mapKey2, mapValue2);

			}

			if (this.k == 0) {
				String s = Character.toString('þ');
				output.collect(new Text(s), new IntWritable(0));
				this.k += 1;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
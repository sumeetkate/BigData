import java.io.IOException;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AirlineOnScheduleHiLowMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	private Text mapKey1 = new Text();
	private IntWritable mapValue1 = new IntWritable();
	private IntWritable mapValue2 = new IntWritable();
	private int i1, i2;
	int k = 0;
	int f = 1;

	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException /* Mapper Function */
	{
		String[] line = new String[29];
		line = value.toString().split(",");
		mapKey1 = new Text(line[8]);

		if (NumberUtils.isNumber(line[14])) {
			i1 = Integer.valueOf(line[14]);

			if (i1 > 5) {
				mapValue1 = new IntWritable(0);
			} else {
				mapValue1 = new IntWritable(1);

			}

			output.collect(mapKey1, mapValue1);

		}

		if (NumberUtils.isNumber(line[15])) {
			i2 = Integer.valueOf(line[15]);

			if (i2 > 5) {
				mapValue2 = new IntWritable(0);
			} else {
				mapValue2 = new IntWritable(1);

			}

			output.collect(mapKey1, mapValue2);

		}

		if (this.k == 0) {
			String s = Character.toString('þ');
			output.collect(new Text(s), new IntWritable(100));
			this.k += 1;
		}
	}
}
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class AirlineHiLowProbOutput extends
		MultipleTextOutputFormat<Text, FloatWritable> {
	protected String generateFileNameForKeyValue(Text key, FloatWritable value,
			String name) {
		return "AirlinesOnSchedule";

	}
}
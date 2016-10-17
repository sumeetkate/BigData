import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

public class FlightAnalysisBigDataDriver {

	public static void main(String args[]) {
		try {
			JobConf conf = new JobConf(FlightAnalysisBigDataDriver.class);
			conf.setJobName("FlightAnalysis");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);
			conf.setMapperClass(AirlineOnScheduleHiLowMapper.class);
			conf.setReducerClass(AirlineOnScheduleHiLowReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(AirlineHiLowProbOutput.class);
			FileInputFormat.setInputPaths(conf,
					new Path[] { new Path(args[0]) });
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
			JobClient.runJob(conf);

			JobConf conf2 = new JobConf(FlightAnalysisBigDataDriver.class);
			conf2.setJobName("FlightAnalysis");
			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(IntWritable.class);
			conf2.setMapperClass(AirportTaxiTimeHiLowMapper.class);
			conf2.setReducerClass(AirportTaxiTimeHiLowReducer.class);
			conf2.setOutputFormat(AirportTaxiTimeHiLowReducerOutput.class);
			FileInputFormat.setInputPaths(conf2,
					new Path[] { new Path(args[0]) });
			FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
			JobClient.runJob(conf2);

			JobConf conf3 = new JobConf(FlightAnalysisBigDataDriver.class);
			conf3.setJobName("FlightAnalysis");
			conf3.setOutputKeyClass(Text.class);
			conf3.setOutputValueClass(IntWritable.class);
			conf3.setMapperClass(HighestCancelReasonMapper.class);
			conf3.setReducerClass(HighestCancelReasonReducer.class);
			conf3.setOutputFormat(HighestCancelReasonOutput.class);
			FileInputFormat.setInputPaths(conf3,
					new Path[] { new Path(args[0]) });
			FileOutputFormat.setOutputPath(conf3, new Path(args[3]));
			JobClient.runJob(conf3);
		}

		catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}

}

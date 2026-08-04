package uni.fmi.AnalyzeExpenses;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ExpensesReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	private String resultType = "Total expenses";

	@Override
	public void configure(JobConf job) {
		resultType = job.get("resultType", "Total expenses");
	}

	@Override
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
			Reporter reporter) throws IOException {

		double sum = 0;
		int count = 0;

		while (values.hasNext()) {
			sum += values.next().get();
			count++;
		}

		double result;

		if ("Average expenses".equals(resultType)) {
			result = sum / count;
		} else {
			result = sum;
		}

		double roundedResult = Math.round(result * 100.0) / 100.0;
		output.collect(key, new DoubleWritable(roundedResult));

	}
}

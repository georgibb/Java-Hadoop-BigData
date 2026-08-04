package uni.fmi.AnalyzeExpenses;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExpensesMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    private String yearFilter = "";
    private String sectorFilter = "";
    private String unitsFilter = "";

    @Override
    public void configure(JobConf job) {

        String filters = job.get("state", "");

        String[] parts = filters.split(";");
        for (String part : parts) {
            String[] keyValue = part.split("=");
            if (keyValue.length != 2) continue;

            String key = keyValue[0].trim();
            String value = keyValue[1].trim();

            if (key.equals("year")) {
                yearFilter = value;
            } else if (key.equals("sector")) {
                sectorFilter = value;
            } else if (key.equals("units")) {
                unitsFilter = value;
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value,
                    OutputCollector<Text, DoubleWritable> output,
                    Reporter reporter) throws IOException {

        String[] columns = value.toString().split(",");

        if (columns.length < 10)
            return;

        String year = columns[0];
        String sector = columns[1];
        String units = columns[5];
        String dataValueStr = columns[8];

        if (!yearFilter.isEmpty() && !year.toLowerCase().contains(yearFilter.toLowerCase()))
            return;

        if (!sectorFilter.isEmpty() && !sector.toLowerCase().contains(sectorFilter.toLowerCase()))
            return;

        if (!unitsFilter.isEmpty() && !units.toLowerCase().contains(unitsFilter.toLowerCase()))
            return;

        try {
            double val = Double.parseDouble(dataValueStr);

            Text outKey = new Text("Сектор: "+ sector + " \n Единици: " + units + " \n Стойност: ");
            output.collect(outKey, new DoubleWritable(val));

        } catch (NumberFormatException e) {
        }
    }
}

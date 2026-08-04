package uni.fmi.AnalyzeExpenses;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class MainFrame extends JFrame {

	private JTextField yearField;
	private JTextField sectorField;
	private JTextField unitsField;
	private JComboBox<String> typeFilter;
	private JTextArea resultField;

	public MainFrame() {
		init();
	}

	private void init() {
		JPanel panel = new JPanel();
		panel.setLayout(null);

		yearField = new JTextField();
		sectorField = new JTextField();
		unitsField = new JTextField();
		JLabel yearLabel = new JLabel("Year:");
		JLabel sectorLabel = new JLabel("Sector:");
		JLabel unitsLabel = new JLabel("Units:");
		JLabel typeLabel = new JLabel("Result Type:");
		String[] resultTypes = { "Total expenses", "Average expenses" };
		typeFilter = new JComboBox<>(resultTypes);
		JButton searchButton = new JButton("Search");
		resultField = new JTextArea();

		yearLabel.setBounds(40, 40, 120, 20);
		yearField.setBounds(160, 40, 150, 30);
		sectorLabel.setBounds(40, 90, 120, 20);
		sectorField.setBounds(160, 90, 150, 30);
		unitsLabel.setBounds(40, 140, 120, 20);
		unitsField.setBounds(160, 140, 150, 30);
		typeLabel.setBounds(40, 190, 120, 20);
		typeFilter.setBounds(160, 190, 150, 30);
		searchButton.setBounds(100, 240, 150, 30);
		resultField.setBounds(40, 290, 270, 300);

		panel.add(yearLabel);
		panel.add(yearField);
		panel.add(sectorLabel);
		panel.add(sectorField);
		panel.add(unitsLabel);
		panel.add(unitsField);

		panel.add(typeLabel);
		panel.add(typeFilter);

		panel.add(searchButton);
		panel.add(resultField);

		add(panel);

		setSize(350, 650);
		setVisible(true);

		searchButton.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				String year = yearField.getText().trim();
				String sector = sectorField.getText().trim();
				String units = unitsField.getText().trim();
				String type = (String) typeFilter.getSelectedItem();

				String filters = "year=" + year + ";" + "sector=" + sector + ";" + "units=" + units + ";" + "type="
						+ type;

				initHadoop(filters);
			}
		});
	}

	protected void initHadoop(String filters) {
		Configuration conf = new Configuration();

		JobConf job = new JobConf(conf, MainFrame.class);

		job.set("state", filters);

		String[] parts = filters.split(";");
		String resultType = "Total expenses";
		for (String p : parts) {
			if (p.startsWith("type=")) {
				resultType = p.substring(5);
			}
		}

		job.set("resultType", resultType);

		job.setMapperClass(ExpensesMapper.class);
		job.setReducerClass(ExpensesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/3.csv");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/result");

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), conf);

			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}

			RunningJob task = JobClient.runJob(job);

			if (task.isComplete()) {

				Path resultFileLocation = new Path("hdfs://127.0.0.1:9000/result/part-00000");

				InputStreamReader stream = new InputStreamReader(fs.open(resultFileLocation));
				BufferedReader reader = new BufferedReader(stream);

				resultField.setText("");
				String line;

				while ((line = reader.readLine()) != null) {
					resultField.append(line + "\n");
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
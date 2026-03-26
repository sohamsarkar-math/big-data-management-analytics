import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MonthTemp {

    public static class TempMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the header row if it exists
            if (key.get() == 0 && value.toString().contains("Region")) {
                return;
            }

            // CSV columns: 0:Region, 1:Country, 2:State, 3:City, 4:Month, 5:Day, 6:Year, 7:AvgTemperature
            String[] columns = value.toString().split(",");

            // Ensure we have all columns and check if Region is "Asia"
            if (columns.length >= 8 && columns[0].trim().equalsIgnoreCase("Asia")) {
                try {
                    String country = columns[1].trim();
                    String month = columns[4].trim();
                    double temp = Double.parseDouble(columns[7].trim());

                    // Filter out common 'missing data' placeholders like -99
                    if (temp > -90) {
                        // We pad the month with a leading zero (e.g., "01" instead of "1")
                        // so that it sorts correctly (1, 2, ... 10, 11) rather than lexicographically
                        if (month.length() == 1) month = "0" + month;

                        outKey.set(country + "\t" + month);
                        outValue.set(temp);
                        context.write(outKey, outValue);
                    }
                } catch (NumberFormatException e) {
                    // Skip rows with invalid temperature data
                }
            }
        }
    }

    public static class TempReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                result.set(sum / count);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageTemp <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Temperature");
        job.setJarByClass(MonthTemp.class);

        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
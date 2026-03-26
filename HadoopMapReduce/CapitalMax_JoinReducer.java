import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CapitalMax_JoinReducer {

    // Helper to clean quotes and whitespace consistently
    private static String clean(String s) {
        if (s == null) return "";
        return s.trim().replace("\"", "").replaceAll("[^\\p{Print}]", "");
    }

    // Mapper for the large city_temperature.csv file
    public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (key.get() == 0 && line.contains("Region")) return;

            String[] cols = line.split(",");
            if (cols.length >= 8) {
                String country = clean(cols[1]);
                String city = clean(cols[3]);
                String temp = clean(cols[7]);

                // We use lowercase for the key to ensure they join regardless of casing
                // Key: country\tcity, Value: Tag "T" + temperature
                outKey.set(country.toLowerCase() + "\t" + city.toLowerCase());
                outValue.set("T:" + temp);
                context.write(outKey, outValue);
            }
        }
    }

    // Mapper for the smaller country-list-3.csv file
    public static class CapitalMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (key.get() == 0 && (line.contains("country") || line.contains("capital"))) return;

            // Regex split to handle commas inside quotes (e.g., "Washington, D.C.")
            String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            if (parts.length >= 2) {
                String country = clean(parts[0]);
                String capital = clean(parts[1]);

                // Key: country\tcity, Value: Tag "C" + original names for display
                outKey.set(country.toLowerCase() + "\t" + capital.toLowerCase());
                outValue.set("C:" + country + "\t" + capital);
                context.write(outKey, outValue);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isCapital = false;
            double maxTemp = Double.NEGATIVE_INFINITY;
            boolean hasTempData = false;
            String displayName = "";

            for (Text val : values) {
                String cell = val.toString();
                if (cell.startsWith("C:")) {
                    // This record confirms the city is a capital
                    isCapital = true;
                    displayName = cell.substring(2); // Extract original country\tcapital
                } else if (cell.startsWith("T:")) {
                    // This is a temperature record
                    try {
                        double temp = Double.parseDouble(cell.substring(2));
                        if (temp > -90) { // Filter missing data
                            if (temp > maxTemp) {
                                maxTemp = temp;
                            }
                            hasTempData = true;
                        }
                    } catch (NumberFormatException e) {
                        // Skip malformed numbers
                    }
                }
            }

            // JOIN CONDITION: Only output if it's a capital AND has temperature data
            if (isCapital && hasTempData) {
                outKey.set(displayName);
                outValue.set(maxTemp);
                context.write(outKey, outValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CapitalMaxTempReducerJoin <temp_input> <capital_input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Reducer-Side Join Capital Max Temp");
        job.setJarByClass(CapitalMax_JoinReducer.class);

        // MultipleInputs allows different mappers for different files
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TemperatureMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, CapitalMapper.class);

        job.setReducerClass(JoinReducer.class);

        // Map output types (must match both Mappers)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Final output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
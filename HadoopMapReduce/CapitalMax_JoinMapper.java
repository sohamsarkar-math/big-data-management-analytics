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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class CapitalMax_JoinMapper {

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Map<String, String> countryToCapital = new HashMap<>();
        private Text outKey = new Text();
        private DoubleWritable outValue = new DoubleWritable();

        // High-intensity cleaning: removes quotes, non-printable characters, and spaces
        private String clean(String s) {
            if (s == null) return "";
            return s.trim()
                    .replace("\"", "")
                    .replaceAll("[^\\p{Print}]", ""); // Removes hidden Mac/Unix artifacts
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                // Using the specific symlink name 'capitals.csv'
                BufferedReader reader = new BufferedReader(new FileReader("capitals.csv"));
                String line;
                while ((line = reader.readLine()) != null) {
                    // Split while ignoring commas inside quotes (for Washington, D.C.)
                    String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                    if (parts.length >= 2) {
                        String country = clean(parts[0]).toLowerCase();
                        String capital = clean(parts[1]).toLowerCase();
                        countryToCapital.put(country, capital);
                    }
                }
                reader.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // Skip the header of the temperature file
            if (key.get() == 0 && (line.contains("Region") || line.contains("Country"))) return;

            String[] cols = line.split(",");
            if (cols.length >= 8) {
                String country = clean(cols[1]);
                String city = clean(cols[3]);

                String countryKey = country.toLowerCase();
                String cityCleaned = city.toLowerCase();

                // If the country exists in our map AND the city matches the capital
                if (countryToCapital.containsKey(countryKey) &&
                        countryToCapital.get(countryKey).equals(cityCleaned)) {
                    try {
                        double temp = Double.parseDouble(clean(cols[7]));
                        if (temp > -90) { // Standard check for missing data
                            outKey.set(country + "\t" + city);
                            outValue.set(temp);
                            context.write(outKey, outValue);
                        }
                    } catch (NumberFormatException e) {
                        // Skip lines with invalid temperature numbers
                    }
                }
            }
        }
    }

    public static class MaxReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double maxTemp = Double.NEGATIVE_INFINITY;
            boolean hasData = false;
            for (DoubleWritable val : values) {
                if (val.get() > maxTemp) maxTemp = val.get();
                hasData = true;
            }
            if (hasData) {
                result.set(maxTemp);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Capital Max Temp Join");
        job.setJarByClass(CapitalMax_JoinMapper.class);

        // This links the HDFS file to a local name 'capitals.csv' inside the Mapper
        job.addCacheFile(new URI("/input/country-list-3.csv#capitals.csv"));

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(MaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
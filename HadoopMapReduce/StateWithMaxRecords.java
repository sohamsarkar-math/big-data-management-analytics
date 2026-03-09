import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StateMaxRecords {

    // -------------------- MAPPER --------------------
    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text state = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(",");

            // Assuming state is column index 3 (4th column)
            if (fields.length > 3) {
                String stateName = fields[3].trim();
                state.set(stateName);
                context.write(state, one);
            }
        }
    }

    // -------------------- COMBINER --------------------
    public static class Combiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    // -------------------- REDUCER --------------------
    public static class Reduce
            extends Reducer<Text,IntWritable,Text,IntWritable> {

        private HashMap<String, Integer> stateCounts = new HashMap<>();
        private int maxCount = 0;

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            stateCounts.put(key.toString(), sum);

            if (sum > maxCount) {
                maxCount = sum;
            }
        }

        // After all reduce calls finish
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Entry<String, Integer> entry : stateCounts.entrySet()) {
                if (entry.getValue() == maxCount) {
                    context.write(new Text(entry.getKey()),
                            new IntWritable(entry.getValue()));
                }
            }
        }
    }

    // -------------------- DRIVER --------------------
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: StateMaxRecords <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "state max records");

        job.setJarByClass(StateMaxRecords.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combiner.class);   // 🔥 Combiner added
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Ensure single reducer to compute global max
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
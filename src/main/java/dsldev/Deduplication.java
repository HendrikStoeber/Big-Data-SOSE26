package dsldev;


import name.JobName;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Deduplication extends Configured implements Tool
{


    public static class Mapper1 extends Mapper<Object, Text, Text, Text>
    {
        private Text outKey = new Text();
        private Text outValue = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] input = value.toString().split("\\s+");

            String a = input[0];
            String b = input[1];
            String cos = input[2];

            if (a.compareTo(b) < 0) {
                outKey.set(a + "\t" + b);
            } else {
                outKey.set(b + "\t" + a);
            }

            outValue.set(cos);
            context.write(outKey, outValue);

        }
    }


    public static class Reducer1 extends Reducer<Text, Text, Text, NullWritable>
    {
        Text outKey = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            double maxCos = Double.NEGATIVE_INFINITY;

            for (Text value : values) {
                double cos = Double.parseDouble(value.toString());

                if (cos > maxCos) {
                    maxCos = cos;
                }
            }

            outKey.set(key.toString() + "\t" + maxCos);
            context.write(outKey,NullWritable.get());
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length!=2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), JobName.getJobName("Deduplication"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(Deduplication.class);
        job.setReducerClass(Deduplication.Reducer1.class);
        job.setMapperClass(Deduplication.Mapper1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(80);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new Deduplication(), args);
        System.exit(exitcode);
    }
}

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
import types.DoublePairWritable;
import types.IntDoubleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

public class DSLDEV extends Configured implements Tool
{

    //Job1
    public static class Mapper1 extends Mapper<Object, Text, Text, DsldevWritable>
    {

        //Struktur für Cleanup
        HashMap<String, IntDoubleWritable> dayStats = new HashMap<>(); //Pro d: Anzahl und Summe aller Preise
        HashMap<StationDayKey, IntDoubleWritable> stationDayStats = new HashMap<>(); //Pro s,d: Anzahl aller Preise, Summe aller Preise

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //Input: s,d,p
            String[] input = value.toString().split(",");
            String day = input[0].substring(0,input[0].indexOf(' '));
            String station = input[1];
            double price = Double.parseDouble(input[2]);

            if(dayStats.get(day) == null)
            {
                IntDoubleWritable val = new IntDoubleWritable(1,price);
                dayStats.put(day,val);
            }
            else {
                dayStats.get(day).addDouble(price);
                dayStats.get(day).addInt(1);
            }


            StationDayKey sdKey = new StationDayKey(station,day);
            if (stationDayStats.get(sdKey) == null)
            {
                IntDoubleWritable val = new IntDoubleWritable(1,price);
                stationDayStats.put(sdKey,val);
            }
            else {
                stationDayStats.get(sdKey).addDouble(price);
                stationDayStats.get(sdKey).addInt(1);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException
        {
           for(StationDayKey key: stationDayStats.keySet())
           {
               IntDoubleWritable valStation = stationDayStats.get(key); //s+d kombination
               IntDoubleWritable valDay = dayStats.get(key.getDay()); //d
               double psd = valStation.getDouble()/valStation.getInt();
               double pd = valDay.getDouble()/valDay.getInt();
               double ddev = ((psd - pd)/ pd);

               Text outKey = new Text(key.getStation());
               DsldevWritable outValue = new DsldevWritable(key.getDay(),psd,pd,ddev,0);
               context.write(outKey,outValue);
           }
        }
    }

    public static class Reducer1 extends Reducer<Text, DsldevWritable, Text, Text>
    {
        Text outKey=new Text();
        Text outValue=new Text();

        public void reduce(Text key, Iterable<DsldevWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<DsldevWritable> valueList = new ArrayList<>();
            double ddevSum = 0;
            int count = 0;

            for (DsldevWritable value: values)
            {
                DsldevWritable copy = new DsldevWritable(value.getDay(),value.getPsd(),value.getPd(),value.getDdev(),value.getRddev());
                valueList.add(copy);
                ddevSum+=value.getDdev();
                count++;
            }

            double addev = ddevSum/count;

            PriorityQueue<DsldevWritable> largestRddev = new PriorityQueue<>(5, Comparator.comparingDouble(DsldevWritable::getRddev));
            PriorityQueue<DsldevWritable> smallestRddev = new PriorityQueue<>(5,Comparator.comparingDouble(DsldevWritable::getRddev).reversed());

            for (DsldevWritable value: valueList)
            {
                double rddev = (value.getDdev()-addev)/addev;

                DsldevWritable record = new DsldevWritable(value.getDay(),value.getPsd(),value.getPd(),value.getDdev(),rddev);

                if (largestRddev.size() < 5)
                {
                    largestRddev.add(record);
                }
                else if (rddev > largestRddev.peek().getRddev())
                {
                    largestRddev.poll();
                    largestRddev.add(record);
                }

                if (smallestRddev.size() < 5)
                {
                    smallestRddev.add(record);
                }
                else if (rddev < smallestRddev.peek().getRddev())
                {
                    smallestRddev.poll();
                    smallestRddev.add(record);
                }
            }

            for (DsldevWritable record: smallestRddev)
            {
                outKey.set(key.toString());
                outValue.set(record.toString());
                context.write(outKey,outValue);
            }
            for (DsldevWritable record: largestRddev)
            {
                outKey.set(key.toString());
                outValue.set(record.toString());
                context.write(outKey,outValue);

            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        if (args.length!=3) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), JobName.getJobName("DSLDEV"));
        int reduceTasks = 1;

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        reduceTasks = Integer.parseInt(args[2]);

        job.setJarByClass(DSLDEV.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DsldevWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Nur eine Aufgabedatei
        job.setNumReduceTasks(50);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new DSLDEV(), args);
        System.exit(exitcode);
    }
}

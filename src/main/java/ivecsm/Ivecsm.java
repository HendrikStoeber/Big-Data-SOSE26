package ivecsm;


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
import java.util.ArrayList;
import java.util.BitSet;

public class Ivecsm extends Configured implements Tool
{

    public static class Mapper1 extends Mapper<Object, Text, IntQuadWritable, IvecsmWritable>
    {
        private IntQuadWritable outKey = new IntQuadWritable();
        private IvecsmWritable outValue = new IvecsmWritable();
        private HyperplaneHash rhh;
        private int hashCount;
        private int bitsPerHash;
        private int partCount;

        private int getPart(String s){return (s.hashCode() & Integer.MAX_VALUE) % partCount;}

        @Override
        public void setup(Mapper.Context context) throws IOException, InterruptedException {
            //Bits per Hash: Anzahl der Linien/Hyperplanes pro Hashcode, Es gibt 2^x mögliche Hashwerte
            //Hash count: Anzahl der Linienkombinationen bzw. Anzahl unterschiedlicher Hashcodes pro Wort
            hashCount = context.getConfiguration().getInt("hashCount",1);
            bitsPerHash = context.getConfiguration().getInt("bitsPerHash",10);
            partCount = context.getConfiguration().getInt("partCount", 2);
            rhh = new HyperplaneHash(hashCount, bitsPerHash, 100);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            //input pro Zeile: Ein Wort und ein Vektor mit 100 Dimensionen, tab getrennt
            String[] input= value.toString().split("\\s+");
            double[] vector = new double[100];

            if (input.length != 101) return;

            for (int i=0;i<100;i++)
            {
                vector[i]= Double.parseDouble(input[i+1]);
            }
            //Normalisieren
            HyperplaneHash.normalizeInPlace(vector);

            BitSet[] hashes = rhh.hash(vector);
            outValue.set(input[0],vector);

            int ownPart = getPart(input[0]);

            for (int bandId = 0; bandId<hashCount; bandId++)
            {
                int hash = bitSetToInt(hashes[bandId], bitsPerHash);
                if (partCount <= 1) {
                    outKey.set(bandId, hash, 0, 0);
                    context.write(outKey, outValue);
                } else {
                    for (int otherPart = 0; otherPart < partCount; otherPart++) {
                        int part1 = Math.min(ownPart, otherPart);
                        int part2 = Math.max(ownPart, otherPart);

                        outKey.set(bandId, hash, part1, part2);
                        context.write(outKey, outValue);
                    }
                }
            }

            //OutKey: BandID? + Hash (Es gibt pro Band sozusagen andere Hashfunktion)
            //OutValue: Wort + Vektor
        }
    }


    public static class Reducer1 extends Reducer<IntQuadWritable, IvecsmWritable, Text, NullWritable>
    {
        private double r;
        private int partCount;

        private int getPart(String s) {return (s.hashCode() & Integer.MAX_VALUE) % partCount;}

        @Override
        public void setup(Reducer.Context context) throws IOException, InterruptedException {
            r = context.getConfiguration().getDouble("r", 0.9);
            partCount = context.getConfiguration().getInt("partCount", 2);
        }

        public void reduce(IntQuadWritable key, Iterable<IvecsmWritable> values, Context context) throws IOException, InterruptedException {
            Text outKey = new Text();

            int part1 = key.getPart1();
            int part2 = key.getPart2();

            ArrayList<IvecsmWritable> list1 = new ArrayList<>();
            ArrayList<IvecsmWritable> list2 = new ArrayList<>();

            for (IvecsmWritable value : values)
            {
                IvecsmWritable copy = new IvecsmWritable(value.getItem(), value.getVector());
                int p = getPart(copy.getItem());

                if (part1 == part2) {
                    // Diagonal-Key: nur Werte dieses einen Parts
                    if (p == part1) {
                        list1.add(copy);
                    }
                } else {
                    // Cross-Key: Werte getrennt einsortieren
                    if (p == part1) {
                        list1.add(copy);
                    } else if (p == part2) {
                        list2.add(copy);
                    }
                }
            }

            if (part1 == part2)
            {
                // Nur innerhalb derselben Part-Gruppe vergleichen
                for (int i = 0; i < list1.size() - 1; i++) {
                    IvecsmWritable a = list1.get(i);

                    for (int j = i + 1; j < list1.size(); j++) {
                        IvecsmWritable b = list1.get(j);

                        double cos = HyperplaneHash.dot(a.getVector(), b.getVector());

                        if (cos > r) {
                            writeResult(a, b, cos, outKey, context);
                        }
                    }
                }
            }
            else
            {
                    // Nur cross vergleichen: part1 x part2
                    for (int i = 0; i < list1.size(); i++)
                    {
                        IvecsmWritable a = list1.get(i);

                        if (i % 1000 == 0)
                        {
                            context.progress();
                        }

                        for (int j = 0; j < list2.size(); j++)
                        {
                            IvecsmWritable b = list2.get(j);

                            double cos = HyperplaneHash.cosineSimilarity(a.getVector(), b.getVector());

                            if (cos > r)
                            {
                                writeResult(a, b, cos, outKey, context);
                            }
                        }
                    }
            }
//            for (int i=0;i<list.size()-1;i++)
//            {
//                IvecsmWritable a = list.get(i);
//                int aPart = getPart(a.getItem());
//
//                for(int j=i+1; j<list.size();j++)
//                {
//                    IvecsmWritable b = list.get(j);
//                    int bPart = getPart(b.getItem());
//
//                    boolean validPair;
//                    if (part1 == part2) {
//                        validPair = aPart == part1 && bPart == part1;
//                    } else {
//                        validPair =
//                                (aPart == part1 && bPart == part2)
//                                        || (aPart == part2 && bPart == part1);
//                    }
//
//                    if (!validPair) {
//                        continue;
//                    }
//
//                    double cos = HyperplaneHash.cosineSimilarity(a.getVector(),b.getVector());
//                    if (cos>r)
//                    {
//                        if(a.getItem().compareTo(b.getItem())<0) outKey.set(a.getItem()+"\t"+b.getItem()+"\t"+cos);
//                        else outKey.set(b.getItem()+"\t"+a.getItem()+"\t"+cos);
//                        context.write(outKey,NullWritable.get());
//                    }
//                }
//            }
                //input: Hash, Vektor Paare
                //Cos-Ähnlichkeit berechnen
                //output: Paare für die Cosinus-Maß<r, r=Param aus Konsole
                //Muster: [item1] [item2] [Cos-Ähn], lexikographisch sortiert


        }
        private void writeResult(IvecsmWritable a,IvecsmWritable b,double cos,Text outKey,Context context) throws IOException, InterruptedException
        {
            if(a.getItem().compareTo(b.getItem())<0) outKey.set(a.getItem()+"\t"+b.getItem()+"\t"+cos);
            else outKey.set(b.getItem()+"\t"+a.getItem()+"\t"+cos);
            context.write(outKey,NullWritable.get());
        }
    }

    private static int bitSetToInt(BitSet bitSet, int bitsPerHash)
    {
        int result = 0;

        for (int i = 0; i < bitsPerHash; i++) {
            if (bitSet.get(i)) {
                result |= (1 << i);
            }
        }

        return result;
    }




    @Override
    public int run(String[] args) throws Exception {
        if (args.length!=7) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf(), JobName.getJobName("IVECSM"));
        int reduceTasks = 1;
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.getConfiguration().setInt("hashCount", Integer.parseInt(args[2]));
        job.getConfiguration().setInt("bitsPerHash", Integer.parseInt(args[3]));
        job.getConfiguration().setDouble("r", Double.parseDouble(args[4]));
        reduceTasks=Integer.parseInt(args[5]);
        job.getConfiguration().setInt("partCount", Integer.parseInt(args[6]));

        job.setJarByClass(Ivecsm.class);
        job.setReducerClass(Ivecsm.Reducer1.class);
        job.setMapperClass(Ivecsm.Mapper1.class);
        job.setMapOutputKeyClass(IntQuadWritable.class);
        job.setMapOutputValueClass(IvecsmWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //Nur eine Ausgabedatei
        job.setNumReduceTasks(reduceTasks);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitcode = ToolRunner.run(new Ivecsm(), args);
        System.exit(exitcode);
    }
}

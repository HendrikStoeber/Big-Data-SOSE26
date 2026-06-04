package dsldev;

import mrtest.BasicWordCount;
import mrtest.MRTestBase;
import name.JobName;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** 
 * Quelle: https://github.com/bobfreitas/minicluster-mr2/ 
 *
 * 
 * [[WINDOWS-NUTZER]]
 * 
 * Es muss winutils.exe und eine hadoop.dll installiert werden. Wie hier 
 * beschrieben: https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems
 * Anschließend ggf jeweils ein "/" zu Beginn der Pfade IN_DIR und DATA_DIR hinzufügen.
 * 
 */

public class DsldevMRTest extends MRTestBase {

    private static String IN_DIR = "testing/wordcount/input";
    private static String OUT_DIR = "testing/wordcount/output";
    private static String DATA_FILE = "sample.txt";

    @Test
    public void testClusterWithData() throws Throwable {
        Path inDir = new Path(IN_DIR);
        Path outDir = new Path(OUT_DIR);
        
        // make sure we can write these files
        fs.delete(inDir, true);
        fs.delete(outDir,true);
        
        // create the input data files
        List<String> content = new ArrayList<String>();
        String A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        String B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

        content.add("2014-06-01 09:50:01+02," + A + ",0.950,0.950,0.950,1,1,1\n");
        content.add("2014-06-01 09:50:01+02," + B + ",1.050,1.050,1.050,1,1,1\n");

        content.add("2014-06-02 09:50:01+02," + A + ",0.900,0.900,0.900,1,1,1\n");
        content.add("2014-06-02 09:50:01+02," + B + ",1.100,1.100,1.100,1,1,1\n");

        content.add("2014-06-03 09:50:01+02," + A + ",0.850,0.850,0.850,1,1,1\n");
        content.add("2014-06-03 09:50:01+02," + B + ",1.150,1.150,1.150,1,1,1\n");

        content.add("2014-06-04 09:50:01+02," + A + ",0.800,0.800,0.800,1,1,1\n");
        content.add("2014-06-04 09:50:01+02," + B + ",1.200,1.200,1.200,1,1,1\n");

        content.add("2014-06-05 09:50:01+02," + A + ",0.750,0.750,0.750,1,1,1\n");
        content.add("2014-06-05 09:50:01+02," + B + ",1.250,1.250,1.250,1,1,1\n");

        content.add("2014-06-06 09:50:01+02," + A + ",0.700,0.700,0.700,1,1,1\n");
        content.add("2014-06-06 09:50:01+02," + B + ",1.300,1.300,1.300,1,1,1\n");

        content.add("2014-06-07 09:50:01+02," + A + ",0.650,0.650,0.650,1,1,1\n");
        content.add("2014-06-07 09:50:01+02," + B + ",1.350,1.350,1.350,1,1,1\n");

        content.add("2014-06-08 09:50:01+02," + A + ",0.600,0.600,0.600,1,1,1\n");
        content.add("2014-06-08 09:50:01+02," + B + ",1.400,1.400,1.400,1,1,1\n");

        content.add("2014-06-09 09:50:01+02," + A + ",0.550,0.550,0.550,1,1,1\n");
        content.add("2014-06-09 09:50:01+02," + B + ",1.450,1.450,1.450,1,1,1\n");

        content.add("2014-06-10 09:50:01+02," + A + ",0.500,0.500,0.500,1,1,1\n");
        content.add("2014-06-10 09:50:01+02," + B + ",1.500,1.500,1.500,1,1,1\n");

        content.add("2014-06-11 09:50:01+02," + A + ",0.450,0.450,0.450,1,1,1\n");
        content.add("2014-06-11 09:50:01+02," + B + ",1.550,1.550,1.550,1,1,1\n");

        content.add("2014-06-12 09:50:01+02," + A + ",0.400,0.400,0.400,1,1,1\n");
        content.add("2014-06-12 09:50:01+02," + B + ",1.600,1.600,1.600,1,1,1");
        writeHDFSContent(fs, inDir, DATA_FILE, content);
        
        // set up the job, submit the job and wait for it complete
        Job job = Job.getInstance(conf, JobName.getJobName("DSLDEV") );
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DsldevWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(DSLDEV.Mapper1.class);
        job.setReducerClass(DSLDEV.Reducer1.class);
        FileInputFormat.addInputPath(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);
        job.waitForCompletion(true);
        assertTrue(job.isSuccessful());
        
        // now check that the output is as expected
        List<String> results = getJobResults(fs, outDir);
        for (String r : results) {
            System.out.println("RESULT: [" + r + "]");
        }
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-01\t1.25\t1.0\t0.25\t-0.5"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-02\t1.3125\t1.0\t0.3125\t-0.375"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-03\t1.375\t1.0\t0.375\t-0.25"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-04\t1.4375\t1.0\t0.4375\t-0.125"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-05\t1.46875\t1.0\t0.46875\t-0.0625"));

// Tankstelle A: 5 größte rddev
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-08\t1.53125\t1.0\t0.53125\t0.0625"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-09\t1.5625\t1.0\t0.5625\t0.125"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-10\t1.625\t1.0\t0.625\t0.25"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-11\t1.6875\t1.0\t0.6875\t0.375"));
        assertTrue(results.contains("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\t2014-06-12\t1.75\t1.0\t0.75\t0.5"));

// Tankstelle B: 5 kleinste rddev
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-01\t0.75\t1.0\t-0.25\t-0.5"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-02\t0.6875\t1.0\t-0.3125\t-0.375"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-03\t0.625\t1.0\t-0.375\t-0.25"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-04\t0.5625\t1.0\t-0.4375\t-0.125"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-05\t0.53125\t1.0\t-0.46875\t-0.0625"));

// Tankstelle B: 5 größte rddev
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-08\t0.46875\t1.0\t-0.53125\t0.0625"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-09\t0.4375\t1.0\t-0.5625\t0.125"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-10\t0.375\t1.0\t-0.625\t0.25"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-11\t0.3125\t1.0\t-0.6875\t0.375"));
        assertTrue(results.contains("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\t2014-06-12\t0.25\t1.0\t-0.75\t0.5"));

        assertEquals(20, results.size());

        // clean up after test case
        fs.delete(inDir, true);
        fs.delete(outDir,true);
        
    }
    
}


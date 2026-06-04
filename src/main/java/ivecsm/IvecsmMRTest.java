package ivecsm;

import mrtest.BasicWordCount;
import mrtest.MRTestBase;
import name.JobName;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;
import types.IntPairWritable;

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

public class IvecsmMRTest extends MRTestBase {

    private static String IN_DIR = "testing/wordcount/input";
    private static String OUT_DIR = "testing/wordcount/output";
    private static String DATA_FILE = "sample.txt";

    @Test
    public void testClusterWithData() throws Throwable {
        Path inDir = new Path(IN_DIR);
        Path outDir = new Path(OUT_DIR);

        conf.setInt("hashCount", 1);
        conf.setInt("bitsPerHash", 10);
        conf.setDouble("r", 0.9);
        conf.setInt("partCount",4);

        // make sure we can write these files
        fs.delete(inDir, true);
        fs.delete(outDir,true);
        
        // create the input data files
        List<String> content = new ArrayList<String>();
        content.add(ivecLine("alpha", 0));
        content.add(ivecLine("beta", 0));
        content.add(ivecLine("gamma", 0));

        content.add(ivecLine("delta", 1));
        content.add(ivecLine("epsilon", 1));

        content.add(ivecLine("zeta", 2));
        writeHDFSContent(fs, inDir, DATA_FILE, content);
        
        // set up the job, submit the job and wait for it complete
        Job job = Job.getInstance(conf, JobName.getJobName("IVECSM") );
        job.setMapOutputKeyClass(IntQuadWritable.class);
        job.setMapOutputValueClass(IvecsmWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(Ivecsm.Mapper1.class);
        job.setReducerClass(Ivecsm.Reducer1.class);
        FileInputFormat.addInputPath(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);
        job.waitForCompletion(true);
        assertTrue(job.isSuccessful());
        
        // now check that the output is as expected
        List<String> results = getJobResults(fs, outDir);
        assertTrue(results.contains("alpha\tbeta\t1.0"));
        assertTrue(results.contains("alpha\tgamma\t1.0"));
        assertTrue(results.contains("beta\tgamma\t1.0"));
        assertTrue(results.contains("delta\tepsilon\t1.0"));
        assertEquals(4, results.size());

        // clean up after test case
        fs.delete(inDir, true);
        fs.delete(outDir,true);
        
    }

    private String ivecLine(String item, int oneAtIndex) {
        StringBuilder sb = new StringBuilder();
        sb.append(item);

        for (int i = 0; i < 100; i++) {
            if (i == oneAtIndex) {
                sb.append("\t1.0");
            } else {
                sb.append("\t0.0");
            }
        }
        sb.append("\n");
        return sb.toString();
    }
}


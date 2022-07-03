import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumofInt {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 2) {
      System.out.printf("Usage: SumofInt <input dir> <output dir>\n");
      System.exit(-1);
    }

    /*
     * Instantiate a Job object for your job's configuration. 
     */
    Configuration config = new Configuration();
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    @SuppressWarnings("deprecation")
    
    Job job = new Job(config, "SumofInt");
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(SumofInt.class);
    job.setMapperClass(MapForSumofInt.class);
    job.setReducerClass(ReduceForSumofInt.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,input);
    FileOutputFormat.setOutputPath(job,output);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("SumofInt");

    /*
     * TODO implement
     */
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}


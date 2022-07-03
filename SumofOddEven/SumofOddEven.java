import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumofOddEven {

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: SumofOddEven <input dir> <output dir>\n");
      System.exit(-1);
    }

    Configuration config = new Configuration();
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    @SuppressWarnings("deprecation")
    
    Job job = new Job(config, "SumofOddEven");
    
    job.setJarByClass(SumofOddEven.class);
    job.setMapperClass(MapForSumofOddEven.class);
    job.setReducerClass(ReduceForSumofOddEven.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,input);
    FileOutputFormat.setOutputPath(job,output);
    
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}


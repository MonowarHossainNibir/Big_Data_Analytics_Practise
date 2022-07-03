import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapForAvgofInt extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  String[] words = line.split(",");
	  for (String word: words){
			  Text outputKey = new Text ("Avg");
			  DoubleWritable outputValue = new DoubleWritable(Double.parseDouble(word));
			  context.write(outputKey, outputValue);
	  }
  }
}

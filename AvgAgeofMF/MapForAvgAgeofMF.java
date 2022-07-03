import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapForAvgAgeofMF extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String doc = value.toString();
	  String[] lines = doc.split("\n");
	  for (String line: lines){
		  String[] words = line.split(",");
		  if(words[0].equals("M")){
			  Text outputKey = new Text ("Male");
			  DoubleWritable outputValue = new DoubleWritable(Double.parseDouble(words[1]));
			  context.write(outputKey, outputValue);
		  }
		  else if(words[0].equals("F")){
			  Text outputKey = new Text ("Female");
			  DoubleWritable outputValue = new DoubleWritable(Double.parseDouble(words[1]));
			  context.write(outputKey, outputValue);
		  }
	  }
  }
}

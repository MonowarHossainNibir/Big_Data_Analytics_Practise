import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapForAgeGroup extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String doc = value.toString();
	  String[] lines = doc.split("\n");
	  for (String line: lines){
		  String[] words = line.split(",");
		  int age = Integer.parseInt(words[1]);
		  if(age>=1 && age<=25){
			  Text outputKey = new Text ("1-25");
			  DoubleWritable outputValue = new DoubleWritable(1);
			  context.write(outputKey, outputValue);
		  }
		  else if(age>=26 && age<=50){
			  Text outputKey = new Text ("26-50");
			  DoubleWritable outputValue = new DoubleWritable(1);
			  context.write(outputKey, outputValue);
		  }
		  else if(age>=51 && age<=75){
			  Text outputKey = new Text ("51-75");
			  DoubleWritable outputValue = new DoubleWritable(1);
			  context.write(outputKey, outputValue);
		  }
		  else if(age>=76 && age<=100){
			  Text outputKey = new Text ("76-100");
			  DoubleWritable outputValue = new DoubleWritable(1);
			  context.write(outputKey, outputValue);
		  }
	  }
  }
}

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceForAvgofInt extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

	  int count = 0;
	  double sum = 0;
	  double avg;
	  
	  for(DoubleWritable value: values){
		  count ++;
		  sum += value.get();
	  } 
	  avg = (sum/count);
	  context.write(key, new DoubleWritable(avg));
  }
}

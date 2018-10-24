package wordcounterpackage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwitterKeyWorkReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String output = "";

        for (Text value : values) {
            output += value + ",";
        }
        context.write(key, new Text(output));
    }
}


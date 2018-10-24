package wordcounterpackage;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
public class TwitterKeyWordMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String tweet = status.getText().toUpperCase();
            String id = "" + status.getId();
            if (tweet.contains("MAGA")) {
                context.write(new Text("MAGA"), new Text(id));
            }
            else if (tweet.contains("DICTATOR")) {
                context.write(new Text("DICTATOR,"), new Text(id));
            }
            else if (tweet.contains("IMPEACH")) {
                context.write(new Text("IMPEACH,"), new Text(id));
            }
            else if (tweet.contains("SWAMP")) {
                context.write(new Text("SWAMP,"), new Text(id));
            }
            else if (tweet.contains("DRAIN")) {
                context.write(new Text("DRAIN,"), new Text(id));
            }
	    else if (tweet.contains("CHANGE")) {
                context.write(new Text("CHANGE,"), new Text(id));
            }
        } catch (TwitterException e) {

        }

    }
}


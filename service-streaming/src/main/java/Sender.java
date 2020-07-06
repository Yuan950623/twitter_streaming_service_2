import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import util.LogUploader;

import java.util.concurrent.LinkedBlockingQueue;

public class Sender {

    public static void main(String[] args) {

        final LinkedBlockingQueue<Status> feedsQueue = new LinkedBlockingQueue<Status>();

        // set up twitter streaming api credentials
        ConfigurationBuilder cb = new ConfigurationBuilder();

        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("pzuMdvcjCoFq6Bp1MPJRbS7V4")
                .setOAuthConsumerSecret("OP7qBJ7AQwYXjeytc1bA4GaTLb7qH28b3vc5cIw0Z6N7RDFh1C")
                .setOAuthAccessToken("1220483156316512264-TiwZr5wo8Ps0R1y1CrrKwJLFefT1g4")
                .setOAuthAccessTokenSecret("Iy27kN82zZd21Q6ANHp2XGNdkssSB96GYo46hdCQahHUE");

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        StatusListener listener = new StatusListener(){

            public void onStatus(Status status) {
                //System.out.println(status.getUser().getName() + " : " + status.getText());
                feedsQueue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        twitterStream.addListener(listener);

        // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        twitterStream.sample();

        for (int i = 0; i < 10009009; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Status curr_feed = feedsQueue.poll();
            if (curr_feed != null) {
                String curr_log = curr_feed.getText();
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("text", curr_log);
                LogUploader.sendLogStream(jsonObject.toJSONString());
            }
        }

    }
}

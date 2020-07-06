package util;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;

public class LogUploader {

    public static void sendLogStream(String log) {
        try {
            URL url = new URL("http://logserver/log");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("clientTime", System.currentTimeMillis() + "");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(("log=" + log).getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println(responseCode);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

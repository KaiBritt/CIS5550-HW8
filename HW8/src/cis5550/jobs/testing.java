package cis5550.jobs;

import cis5550.kvs.Row;
import cis5550.tools.Hasher;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;

public class testing {

    public static void main (String[] args) throws IOException {
        URL url = URI.create("http://advanced.crawltest.401.cis5550.net:80/c8Rhi6R/ZLk8MCK3bju5WkvgW.html").toURL();
        HttpURLConnection headConn = (HttpURLConnection) url.openConnection();
        headConn.setRequestProperty("User-Agent", "cis5550-crawler");
        headConn.setRequestMethod("HEAD");
        String contentLength = null;
        String contentType =  null;
        String location =  null;
        System.out.println("trying to connect");

        headConn.connect();
        System.out.println(headConn.getResponseCode());

    }
}

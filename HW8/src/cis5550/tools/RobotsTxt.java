package cis5550.tools;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.Set;

public class RobotsTxt implements Serializable {
    private URL url;
    private String userAgent;
    private Set<String> disallow = new HashSet<>();
    private Set<String> allow = new HashSet<>();
    private String crawlDelay = "1";


    public RobotsTxt(URL url, String userAgent) {
        this.url = url;
    }

    public boolean loadRobots() throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestProperty("User-Agent", userAgent);
        conn.setRequestMethod("GET");
        if (conn.getResponseCode() == 200) {
            try (InputStream in = conn.getInputStream()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line;
                String curAgent = "";
                boolean ourAgent = false;
                while ((line = reader.readLine()) != null){
                    if (line.startsWith("User-agent")){
                        curAgent = line.split(":")[1].strip();
                        if (curAgent.equals(userAgent) || curAgent.equals("*")){
                            ourAgent = true;
                        }
                        else {
                            ourAgent = false;
                        }
                    }
                    if (line.startsWith("Disallow") && ourAgent){
                        disallow.add(line.split(":")[1].strip());
                    }
                    else if (line.startsWith("Allow") && ourAgent){
                        allow.add(line.split(":")[1].strip());
                    }
                    else if (line.startsWith("Crawl-delay") && ourAgent){
                        crawlDelay = line.split(":")[1].strip();
                    }
                }
            }
            return true;
        }
        else {
            return false;
        }
    }

    public String getCrawlDelay(){
        return crawlDelay;
    }

    public boolean validPath(String path){
       for (String bannedPath : disallow){
           if (path.startsWith(bannedPath)){
               return false;
           }
       }
        for (String allowedPath : allow){
            if (path.startsWith(allowedPath)){
                return true;
            }
        }
        return false;
    }

    public static byte[] serialize(RobotsTxt robot) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(robot);
            out.flush();
            return bos.toByteArray();
        }
    }

    public static RobotsTxt deserialize(byte[] data) throws IOException, ClassNotFoundException {
        if (data == null){
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (RobotsTxt) in.readObject();
        }
    }

}

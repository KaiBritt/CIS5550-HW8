package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.RobotsTxt;
import cis5550.tools.URLParser;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {
    final static int SEED_LIMIT = 1;
    final static String USER_AGENT = "cis5550-crawler";
  private static List<String> extractTags(String html){
        ArrayList<String> taglist = new ArrayList<>();
        boolean openTag  = false;
        StringBuilder curTag = new StringBuilder();
        for(int i = 0; i < html.length(); i++){
            if (html.charAt(i) == '<') {
                openTag = true;
                continue;
            }
            if (html.charAt(i) == '>'){
                taglist.add(curTag.toString());
                curTag.setLength(0);
                openTag = false;
            }
            else if (openTag){
                curTag.append(html.charAt(i));
            }

        }
        return taglist;
    }


    // come back and change this
    private static List<String> extractUrls(List<String> tagList) {
        List<String> urls = new ArrayList<>();
        Pattern hrefPattern = Pattern.compile("href\\s*=\\s*['\"]([^'\"]+)['\"]", Pattern.CASE_INSENSITIVE);
        for (String tag : tagList) {
              if (!tag.strip().toLowerCase().startsWith("a")) continue;
            Matcher m = hrefPattern.matcher(tag);
            if (m.find()) {
                urls.add(m.group(1)); // the URL inside quotes
            }
        }
        return urls;
    }



    private static String normalizeUrl(String url, String hostUrl, String hostPath){
        String[] parsedUrl = URLParser.parseURL(url);
        // if we have a relative url
        if (parsedUrl[3].indexOf("#") > 0){
            parsedUrl[3] = parsedUrl[3].substring(0, parsedUrl[3].indexOf("#"));
        }
        String normalizedPath = "";

        ArrayDeque<String> path = normalizePath(hostPath, parsedUrl[3]);
        normalizedPath = "/" + String.join("/", path);

        if (parsedUrl[0] == null) {
            return hostUrl + normalizedPath;
        } else if (parsedUrl[1] != null) {
            if (parsedUrl[2] == null){  parsedUrl[2] =  parsedUrl[1].equals("https") ? "443" : "80";}
            return parsedUrl[0] + "://" + parsedUrl[1] + ":" + parsedUrl[2] + normalizedPath;
        }
        else{
            System.out.println("failed to normalize url check this case: " + url);
            return url;
        }

    }

    private static ArrayDeque<String> normalizePath(String hostPath, String newPath) {
        String[] urlPieces = newPath.split("/");
        ArrayDeque<String> path = new ArrayDeque<>();

        // relative path
        if (newPath.startsWith(".") && hostPath != null) {
            for (String hostPiece : hostPath.split("/")) {
                if (hostPiece.isEmpty()) continue;
                path.addLast(hostPiece);
            }
        }

        for (String piece : urlPieces){
            if(piece.isEmpty()) continue;
            if (piece.equals("..")){
                if (!path.isEmpty()) path.pollLast();
            }
            else if (!piece.equals(".")) {
                path.addLast(piece);
            }
        }
        return path;
    }

    private static List<String> normalizeUrls(List<String> urls, String hostUrl, String hostPath){
        List<String> normalizedUrls = new ArrayList<>();
        for( String url : urls){
            normalizedUrls.add(normalizeUrl(url,hostUrl,hostPath));
        }
        return normalizedUrls;
    }

    private static List<String> robotComplience (List<String> urls, RobotsTxt robot){
        List<String> complientUrls = new ArrayList<>();
        for( String url : urls){
            if (robot.validPath( URI.create(url).getPath()))
                complientUrls.add(url);
        }
        return  complientUrls;
    }

    public static  String replaceWildCards(String stringToConvert){
        if (!stringToConvert.endsWith("*")){
            stringToConvert += "$";
        }
        return "^"+stringToConvert.replace("*","[^/]*").replace("/","\\/");
    }

    public static boolean urlBlackListed(String test,String urlToTest ){
        return Pattern.matches(replaceWildCards(test),urlToTest);
    }

    public static  void run(FlameContext ctx, String[] args) throws Exception {
        if (args.length > SEED_LIMIT || args.length == 0){ // only allowing one seed
            ctx.output("No seed was passed");
            return;
        }
        else{
            ctx.output("Starting crawl from:\n");
            for (String arg : args) ctx.output(arg);
        }

        System.out.println(args[0]  + " " +  normalizeUrl(args[0], null, null));
        List<String> normalizedSeedUrls = Arrays.stream(args)
                .map(url -> normalizeUrl(url, null, null)).toList();

        FlameRDD urlQueue = ctx.parallelize(normalizedSeedUrls);
        int iteration = 0;
        // will eventually endup in a while



        while (true) {
            urlQueue = urlQueue.flatMap(s -> {

                // check if url is on the blacklist
                for (Iterator<Row> it = ctx.getKVS().scan("blacklist"); it.hasNext(); ) {
                    Row row = it.next();
                    String pattern = row.get("pattern");
                    if (pattern != null && urlBlackListed(pattern, s)){
                        return new ArrayList<String>();
                    }
                }

                if (ctx.getKVS().existsRow("pt-crawl", Hasher.hash(s))) {
                    return new ArrayList<String>();
                }


                // check and wrtie last time host has been pinged

                URL url = URI.create(s).toURL();
                // checking protocols
                String protocol = url.getProtocol().toLowerCase();
                String path = url.getPath().toLowerCase();
                int hostPort = url.getPort() == -1 ? url.getDefaultPort() : url.getPort();

                String hostName = url.getHost();
                String hostUrl = protocol + "://" + hostName + ":" + hostPort;
                List<String> badExts = Arrays.asList(".jpg", ".jpeg", ".gif", ".png", ".txt");

                boolean badProtocol = !(protocol.equals("http") || protocol.equals("https"));
                boolean badExtension = badExts.stream().anyMatch(path::endsWith);

                if (badProtocol || badExtension) {
                    return new ArrayList<String>();
                }
                // reading robots.txt entry

                RobotsTxt robots = null;
                byte[] robotCol = ctx.getKVS().get("hosts", url.getHost(),"robots");
                if (robotCol == null || !new String(robotCol).equals("no robots")){
                robots = RobotsTxt.deserialize(robotCol);
                // check rate limit
                    if (robots == null ) {      // never accessed host so download robots
                        robots = new RobotsTxt(URI.create(hostUrl + "/robots.txt").toURL() , USER_AGENT);
                        if (robots.loadRobots()){
                            ctx.getKVS().put("hosts",  url.getHost(), "robots", RobotsTxt.serialize(robots));
                            ctx.getKVS().put("hosts",  url.getHost(), "lastAccess", ""+0);

                        }
                        else{
                            ctx.getKVS().put("hosts",  url.getHost(), "robots", "no robots");
                        }
                    }
                }
                byte[] lastAccess = ctx.getKVS().get("hosts", url.getHost(),"lastAccess");
                long crawlDelay;
                if (robots != null ){
                    crawlDelay = (long) (Double.parseDouble(robots.getCrawlDelay()) * 1000) ;
                } else {
                    crawlDelay = 1000;
                }
                if ( lastAccess != null && System.currentTimeMillis()  - Long.parseLong(new String(lastAccess))  < crawlDelay){
                    return List.of(s);
                }

                HttpURLConnection headConn = (HttpURLConnection) url.openConnection();
                headConn.setRequestProperty("User-Agent", USER_AGENT);
                headConn.setRequestMethod("HEAD");
                String contentLength = null;
                String contentType =  null;
                String location =  null;
                Row pageRow = new Row(Hasher.hash(s));
                pageRow.put("url", s);

                headConn.connect();
                ctx.getKVS().put("hosts", url.getHost(), "lastAccess", ""+System.currentTimeMillis());
                HttpURLConnection.setFollowRedirects(false);
                int resCode = headConn.getResponseCode();
                System.out.println(resCode);
                pageRow.put("responseCode", ""+resCode);
                System.out.println("starting to enter " + resCode + " " + hostUrl + url.getPath() );
                if ( resCode == 200 || (resCode > 300  &&  resCode <309)) {
                    contentLength = headConn.getHeaderField("content-length");
                    pageRow.put("length", contentLength);

                    contentType =  headConn.getHeaderField("content-type");
                    pageRow.put("contentType", contentType);

                    location =  headConn.getHeaderField("location");
                } else {
                    ctx.getKVS().putRow("pt-crawl", pageRow);
                    return new ArrayList<String>();
                }


                String hostPath = url.getPath();
                if ((resCode > 300  &&  resCode < 309) && location != null){
                    System.out.println("Exiting redirect " + resCode + " points to " + normalizeUrl(location, hostUrl, hostPath) );

                    ctx.getKVS().putRow("pt-crawl", pageRow);
                    return Collections.singleton(normalizeUrl(location, hostUrl, hostPath));
                }

                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestProperty("User-Agent", USER_AGENT);
                conn.setRequestMethod("GET");
                System.out.println("trying get " + resCode + " " + contentType );

                if (conn.getResponseCode() == 200 && (contentType == null || contentType.equalsIgnoreCase("text/html"))) {

                    byte[] pageData;
                    try (InputStream in = conn.getInputStream()) {
                        pageData = in.readAllBytes();
                    }
                    String pageDataStr = new String(pageData, StandardCharsets.UTF_8);
                    String hashedPageData = Hasher.hash(pageDataStr);

                    Row seenTestRow = ctx.getKVS().getRow("pt-content-seen", hashedPageData);
                    if (seenTestRow != null) {
                        pageRow.put("canonicalURL", seenTestRow.get("url"));
                        ctx.getKVS().putRow("pt-crawl", pageRow);
                        System.out.println("contentSeen " + s + " " + seenTestRow.get("url"));
                        return new ArrayList<String>();
                    }


                    pageRow.put("page", pageData);
                    ctx.getKVS().putRow("pt-crawl", pageRow);
                    Row contentSeenRow = new Row(hashedPageData);
                    contentSeenRow.put("url", s);
                    ctx.getKVS().putRow("pt-content-seen", contentSeenRow);


                    List<String> tags = extractTags(pageDataStr);
                    List<String> urls = extractUrls(tags); // for UTF-8 encoding));
                    List<String> normalizedUrls = normalizeUrls(urls, hostUrl, hostPath);
                    List<String> robotComplientUrls = robotComplience(normalizedUrls, robots);
                    for (String u : robotComplientUrls) System.out.println(u);
                    return robotComplientUrls;
                }
                else {
                    ctx.getKVS().putRow("pt-crawl", pageRow);
                    return new ArrayList<String>();
                }
            });

            if (urlQueue.count() == 0) break;
        }


    }
}

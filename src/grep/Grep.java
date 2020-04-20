package grep;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import concurrent.FixedThreadPool;

public class Grep {

    private static final int BUFFER_SIZE = 8192;
    private static final int ARRAY_SIZE = 8192;
    private static final int NUM_THREADS = 16;

    private static String pattern;
    private static int[] lps;
    private static int m;   //m is pattern length

    public static void main(String[] args) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        if(args.length!=2) {
            System.out.println("Usage: Grep pattern <file>");
            System.exit(0);
        }

        String fileName = args[1];

        File file = new File(fileName);
        if(!file.exists()) {
            System.out.println("Error: " + fileName + " : No such file or directory !");
            System.exit(0);
        }

        pattern = args[0];
        m = pattern.length();
        lps = computeLPS(pattern);

            if(!file.isDirectory()) {
                findPattern(file.getPath());
            } else {
                ExecutorService executorService = new FixedThreadPool(NUM_THREADS);
                processFile(file,executorService);
                executorService.shutdown();
                executorService.awaitTermination(60, TimeUnit.MINUTES);
            }
            System.out.print("Time taken : " + (System.currentTimeMillis() - startTime));
    }

    private static void processFile(File file,ExecutorService executorService) {
        File[] files = file.listFiles();
        if(files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    processFile(f, executorService);
                } else {
                    executorService.execute(() -> findPattern(f.getPath()));
                }
            }
        }
    }

    private static void findPattern(String fileName) {
        int j = 0 ,n ,noOfOccurrence = 0 , i;
        byte[] text = new byte[ARRAY_SIZE];

        try(FileInputStream fis = new FileInputStream(fileName)) {
            while((n=fis.read(text,0,ARRAY_SIZE))!=-1) {
                i = 0;
                while (i < n) {
                    if (pattern.charAt(j) == (char)text[i]) {
                        j++;
                        i++;
                    }
                    if (j == m) {
                        noOfOccurrence++;
                        j = lps[j-1];
                    } else if (i < n && pattern.charAt(j) != (char)text[i]) {
                        if (j != 0)
                            j = lps[j - 1];
                        else
                            i++;
                    }
                }
            }
            System.out.println(fileName + " : " + noOfOccurrence);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int[] computeLPS(String pattern) {
        int len = 0 , i = 1;
        int[] lps = new int[m];
        lps[0] = 0;
        while (i < m) {
            if (pattern.charAt(i) == pattern.charAt(len)) {
                lps[i++] = ++len;
            } else if (len != 0) {
                len = lps[len - 1];
            } else {
                lps[i++] = len;
            }
        }
        return lps;
    }
}
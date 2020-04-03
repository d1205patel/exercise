package grep;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.*;

public class GrepInFolder {

    private static final int BUFFER_SIZE = 8192;
    private static final int ARRAY_SIZE = 8192;
    private static final int NUM_THREADS = 6;

    private static String pattern;
    private static int[] lps;
    private static int noOfLinesInPattern;
    private static int m;   //m is pattern length

    public static void main(String[] args) throws InterruptedException {
        if(args.length!=2) {
            System.out.println("Usage: GrepInFolder pattern <folder>");
            System.exit(0);
        }

        String directoryName = args[1];

        File directory = new File(directoryName);
        if(!directory.isDirectory()) {
            System.out.println("Error: " + directoryName + " is not directory !");
            System.exit(0);
        }

        pattern = args[0];
        lps = computeLPS(pattern);
        noOfLinesInPattern = noOfLines(pattern);
        m = pattern.length();

        try(BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(System.out))) {
            ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
            Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attr) {
                    executorService.execute(() ->findPattern(path.toString(),bufferedWriter));
                    return FileVisitResult.CONTINUE;
                }
            });
            executorService.shutdown();
            executorService.awaitTermination(1,TimeUnit.MINUTES);
            bufferedWriter.flush();
        } catch (IOException e) {
            System.out.println("Error occurred !");
        }
    }

    private static void findPattern(String fileName,BufferedWriter bufferedWriter) {
        int j = 0 ,lines = 1 ,n;
        char[] text = new char[ARRAY_SIZE];

        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName),BUFFER_SIZE)) {
            while ((n = bufferedReader.read(text, 0, ARRAY_SIZE)) != -1) {
                int i = 0;
                while (i < n) {
                    if (text[i] == '\n') {
                        lines++;
                    }
                    if (pattern.charAt(j) == text[i]) {
                        j++;
                        i++;
                    }
                    if (j == m) {
                        bufferedWriter.write( fileName + " : " + (lines - noOfLinesInPattern) + "\n");
                        j = lps[j-1];
                    } else if (i < n && pattern.charAt(j) != text[i]) {
                        if (j != 0)
                            j = lps[j - 1];
                        else
                            i++;
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error occurred while processing file" + fileName);
        }
    }

    private static int[] computeLPS(String pattern) {
        int m = pattern.length(), len = 0 , i = 1;
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

    private static int noOfLines(String s) {
        if(s==null) {
            return 0;
        }
        int numLines = 0;
        for(int i=0;i<s.length();i++) {
            if(s.charAt(i)=='\n') {
                numLines++;
            }
        }
        return numLines;
    }
}

/*
multithreading only helps when there are files with large size
*/
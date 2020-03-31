package grep;

import java.io.*;

public class SimpleGrep {

    private static final int BUFFER_SIZE = 8192;
    private static final int ARRAY_SIZE = 8192;

    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage : SimpleGrep pattern <filename>");
            System.exit(0);
        }

        String fileName = args[1];

        try(BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName),BUFFER_SIZE)) {
            findPattern(bufferedReader,args[0]);
        } catch (FileNotFoundException e) {
            System.out.println("Error: " + fileName + " : No such file");
        } catch (IOException e) {
            System.out.println("Error occurred !");
        }
    }

    private static void findPattern(BufferedReader bufferedReader,String pattern) throws IOException {
        int[] lps = computeLPS(pattern);
        int numLinesInPattern = noOfLines(pattern);
        int m = pattern.length() , j = 0 ,lines = 1 ,n;
        char[] text = new char[ARRAY_SIZE];
        int lastFoundLine = -1;
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(System.out))) {
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
                        if (lastFoundLine != lines) {
                            bufferedWriter.write((lines - numLinesInPattern) + "\n");
                            lastFoundLine = lines;
                        }
                        j = lps[j - 1];
                    } else if (i < n && pattern.charAt(j) != text[i]) {
                        if (j != 0)
                            j = lps[j - 1];
                        else
                            i++;
                    }
                }
            }
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
package grep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class SimpleGrep {

    private static final int BUFFER_SIZE = 8192;
    private static final int ARRAY_SIZE = 8192;

    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage : SimpleGrep <filename> pattern");
            System.exit(0);
        }

        String fileName = args[1];
        BufferedReader bufferedReader = null;

        try {
            bufferedReader = new BufferedReader(new FileReader(fileName),BUFFER_SIZE);

            int matchedLineNumber = findPattern(bufferedReader,args[0]);

            if(matchedLineNumber!=-1) {
                System.out.println(matchedLineNumber);
            }

        } catch (FileNotFoundException e) {
            System.out.println("Error: " + fileName + " : No such file");
        } catch (IOException e) {
            System.out.println("Error while reading file: " + fileName);
        } finally {
            try {
                if(bufferedReader!=null) {
                    bufferedReader.close();
                }
            } catch(IOException e) {
                System.out.println("Error while closing a file");
            }
        }
    }

    private static int findPattern(BufferedReader bufferedReader,String pattern) throws IOException {
        int[] lps = computeLPS(pattern);
        int numLinesInPattern = noOfLines(pattern);
        int m = pattern.length() , j = 0 ,lines = 1 ,n;
        char[] text = new char[ARRAY_SIZE];

        while((n=bufferedReader.read(text,0,ARRAY_SIZE))!=-1) {
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
                    return lines-numLinesInPattern;
                } else if (i < n && pattern.charAt(j) != text[i]) {
                    if (j != 0)
                        j = lps[j - 1];
                    else
                        i++;
                }
            }
        }
        return -1;
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
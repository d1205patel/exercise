package sharding;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Shard {
    private static String DB_STORAGE_PATH = "/Users/pateldarshankumar/Desktop/wikipedia2json/db/";

    private BufferedWriter bufferedWriter;
    private int shardNumber;
    private Path filePath;

    Shard(int i) {
        shardNumber = i;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(DB_STORAGE_PATH + "shard" + i));
            filePath = Paths.get(DB_STORAGE_PATH+"shard"+i);
        } catch (IOException e) {
            System.out.println("Could not start shard" + i);
        }
    }

    public void add(String s) {
        try {
            bufferedWriter.write(s);
        } catch (IOException e) {
            System.out.println("Could not write to shard" + shardNumber);
        }
    }

    public String getJSONObjectAtLine(long lineNumber) {
        String s="";
        try (Stream<String> lines = Files.lines(filePath)) {
            s = lines.skip(lineNumber-1).findFirst().get();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return s;
    }

    public void exit() {
        if(bufferedWriter!=null) {
            try {
                bufferedWriter.close();
            } catch (IOException e) {
                System.out.println("Error occurred while closing shard" + shardNumber);
            }
        }
    }
}

package sharding;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Shard {
    private static String DB_STORAGE_PATH = "/Users/pateldarshankumar/Desktop/wikipedia2json/db/";

    private BufferedWriter bufferedWriter;
    private int shardNumber;

    Shard(int i) {
        shardNumber = i;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(DB_STORAGE_PATH + "shard" + i));
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

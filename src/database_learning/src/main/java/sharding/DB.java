package sharding;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class DB {
    private int numberOfShards;
    private Shard[] shards;
    private Index[] indexes;

    DB(int numberOfShards) {
        this.numberOfShards = numberOfShards;
        shards = new Shard[numberOfShards];
        indexes = new Index[numberOfShards];
        for(int i=0;i<numberOfShards;i++) {
            shards[i] = new Shard(i);
            indexes[i] = new Index();
        }
    }

    public void insertDataFromJSONFile(String filePath) {
        JSONParser jsonParser = new JSONParser();

        try(FileReader fileReader = new FileReader(filePath)) {
            Object ob = jsonParser.parse(fileReader);
            JSONArray objectList = (JSONArray)ob;

            for(Object o:objectList) {
                JSONObject jsonObject = (JSONObject) o;
                long id = (long)jsonObject.get("id");
                int shardNumber = (int)(id%numberOfShards);
                shards[shardNumber].add(jsonObject.toJSONString()+"\n");
                String title = (String)jsonObject.get("title");
                indexes[shardNumber].addString(title,id);
            }
        } catch (FileNotFoundException e) {
            System.out.println("File Not found");
        } catch (IOException e) {
            System.out.println("IOException occurred!");
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public void searchTitle(String s) {
        for(int i=0;i<numberOfShards;i++) {
            List<Long> list = indexes[i].searchString(s);
            if(list!=null) {
                for(Long id:list) {
                    System.out.println(id);
                }
            }
        }
    }

    public void exit() {
        for(int i=0;i<numberOfShards;i++) {
            shards[i].exit();
        }
    }
}

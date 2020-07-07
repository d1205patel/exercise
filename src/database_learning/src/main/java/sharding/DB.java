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
    private TitleIndex[] titleTitleIndices;
    private IdIndex[] idIndexes;

    DB(int numberOfShards) {
        this.numberOfShards = numberOfShards;
        shards = new Shard[numberOfShards];
        titleTitleIndices = new TitleIndex[numberOfShards];
        idIndexes = new IdIndex[numberOfShards];
        for(int i=0;i<numberOfShards;i++) {
            shards[i] = new Shard(i);
            titleTitleIndices[i] = new TitleIndex();
            idIndexes[i] = new IdIndex();
        }
    }

    public void insertDataFromJSONFile(String filePath) {
        JSONParser jsonParser = new JSONParser();

        try(FileReader fileReader = new FileReader(filePath)) {
            Object ob = jsonParser.parse(fileReader);
            JSONArray objectList = (JSONArray)ob;

            long[] currentLine = new long[numberOfShards];

            for(Object o:objectList) {
                JSONObject jsonObject = (JSONObject) o;
                long id = (long)jsonObject.get("id");
                int shardNumber = (int)(id%numberOfShards);
                shards[shardNumber].add(jsonObject.toJSONString()+"\n");
                idIndexes[shardNumber].add(id,++currentLine[shardNumber]);
                String title = (String)jsonObject.get("title");
                titleTitleIndices[shardNumber].addString(title,id);
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
            List<Long> list = titleTitleIndices[i].searchString(s);
            if(list!=null) {
                for(Long id:list) {
                    System.out.println(id);
                }
            }
        }
    }

    public void searchId(Long id) {
        int shardNumber = (int) (id%numberOfShards);
        long lineNumber = idIndexes[shardNumber].get(id);
        if(lineNumber==-1) {
            System.out.println("Not found id = " + id);
        } else {
            System.out.println("Id=" + id + " is found at line number " + lineNumber);
        }
    }

    public void exit() {
        for(int i=0;i<numberOfShards;i++) {
            shards[i].exit();
        }
    }
}

package sharding;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Index {
    private Map<String, LinkedList<Long>> map;

    Index() {
        map = new HashMap<>();
    }

    public void addString(String s, long id) {
        LinkedList list = map.get(s);
        if(list==null) {
            list = new LinkedList();
        }
        if(list.size()!=0 && id == (long)list.getLast()) {
            return;
        }
        list.add(id);
        map.put(s,list);
    }

    public List<Long> searchString(String s) {
        return map.get(s);
    }
}

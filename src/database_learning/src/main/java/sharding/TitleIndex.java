package sharding;

import java.util.HashMap;
import java.util.Map;

public class TitleIndex {
    private Map<String, Long> map;

    TitleIndex() {
        map = new HashMap<>();
    }

    public void addString(String s, long id) {
        map.put(s,id);
    }

    public long getId(String s) {
        Object o = map.get(s);
        return o==null ? -1l:(long)o;
    }
}

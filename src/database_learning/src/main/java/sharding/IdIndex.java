package sharding;

import java.util.HashMap;
import java.util.Map;

public class IdIndex {
    private Map<Long,Long> map;

    IdIndex() {
        map = new HashMap<>();
    }

    public void add(Long id,Long lineNumber) {
        map.put(id,lineNumber);
    }

    public long get(Long id) {
        Object o = map.get(id);
        return o==null? -1:(long)o;
    }
}

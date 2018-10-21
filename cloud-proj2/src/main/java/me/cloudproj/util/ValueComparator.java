package me.cloudproj.util;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mrleit on 4/06/17.
 */
public class ValueComparator implements Comparator<String> {

    HashMap<String, Integer> map = new HashMap<>();

    public ValueComparator(Map<String, Integer> map) {
        this.map.putAll(map);
    }

    @Override
    public int compare(String s1, String s2) {
        if (map.get(s1) >= map.get(s2)) {
            return -1;
        } else {
            return 1;
        }
    }

}

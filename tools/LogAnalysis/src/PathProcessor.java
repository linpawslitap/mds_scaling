/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/19/13
 * Time: 4:29 PM
 * To change this template use File | Settings | File Templates.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class PathProcessor implements  Processor {

    HashMap<String, Integer> count;
    Histogram count_list;
    Histogram max_list;
    Histogram min_list;
    Histogram load_var_list;
    long interval;

    public PathProcessor(long interval) {
        count = new HashMap<String, Integer>();
        max_list = new Histogram();
        min_list = new Histogram();
        count_list = new Histogram();
        load_var_list = new Histogram();
        this.interval = interval;
    }

    public void startWindow() {
        count.clear();
    }

    public void process(Operation op,
                        Set<String> prev_window,
                        Set<String> curr_window) {
        if (count.containsKey(op.src)) {
          count.put(op.src, count.get(op.src)+1);
        } else {
          count.put(op.src, 1);
        }
        if (op.dst != null) {
            if (count.containsKey(op.dst)) {
              count.put(op.dst, count.get(op.dst)+1);
            } else {
              count.put(op.dst, 1);
            }
        }
    }

    public void finishWindow() {
        int max_cnt = 0;
        int min_cnt = 0;
        int tot_count = 0;
        for (Integer v : count.values()) {
            max_cnt = Math.max(v, max_cnt);
            min_cnt = Math.min(v, min_cnt);
            tot_count += v;
        }
        double avg_load_variance = 0.0;
        double avg_load = 1 / (double) count.size();
        for (Integer v : count.values()) {
            avg_load_variance += Math.abs((double) v / tot_count - avg_load);
        }
        avg_load_variance /= count.size();
        if (tot_count > 0) {
            count_list.add((double) count.size() / interval);
            max_list.add((double) max_cnt / tot_count);
            min_list.add((double) min_cnt / tot_count);
            load_var_list.add(avg_load_variance);
        }
    }

    public void print() {
        System.out.println("Path average rate\n"+count_list);
        System.out.println("Path max load\n"+max_list);
        System.out.println("Path min load\n"+min_list);
        System.out.println("Path avg load variance\n"+load_var_list);
    }
}

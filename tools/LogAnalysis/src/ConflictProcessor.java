import java.util.ArrayList;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/19/13
 * Time: 4:44 PM
 * To change this template use File | Settings | File Templates.
 */

public class ConflictProcessor implements Processor {

    Histogram rate_list;
    int tot_ops, num_conflict_ops;
    long interval;

    public ConflictProcessor(long interval) {
        rate_list = new Histogram();
        tot_ops = 0;
        num_conflict_ops = 0;
        this.interval = interval;
    }

    public void startWindow() {
        tot_ops = 0;
        num_conflict_ops = 0;
    }

    boolean findRecursiveConflict(Set<String> window, String path) {
      int pos = 0;
      if (window.contains(path))
        return true;
      while ((pos = path.indexOf('/', pos+1)) > 0) {
        if (window.contains(path.substring(0, pos)))
            return true;
      }
      return false;
    }

    boolean findConflict(Set<String> window, Operation op) {
        boolean result = findRecursiveConflict(window, op.src) ||
               (op.dst != null) && findRecursiveConflict(window, op.dst);
        return result;
    }

    public void process(Operation op,
                        Set<String> prev_window,
                        Set<String> curr_window) {
        if (findConflict(prev_window, op)) {
            num_conflict_ops += 1;
        } else
        if (findConflict(curr_window, op)) {
            num_conflict_ops += 1;
        }
        tot_ops += 1;
    }

    public void finishWindow() {
        if (tot_ops > 0)
            rate_list.add((double) num_conflict_ops / tot_ops);
    }

    public void print() {
        System.out.println("Conflict rate\n"+rate_list);
    }
}


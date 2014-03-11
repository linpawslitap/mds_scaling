/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/19/13
 * Time: 3:34 PM
 * To change this template use File | Settings | File Templates.
 */

public class Operation {
    long time;
    int cmd;
    int rw_type;
    String src;
    String dst;

    public Operation(long start_time, int op, int type, String path1, String path2) {
        time = start_time;
        rw_type = type;
        cmd = op;
        src = path1;
        dst = path2;
    }

    public String toString() {
      return cmd + " " + rw_type + " " + src + " " + dst;
    }
}

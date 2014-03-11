import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/19/13
 * Time: 6:19 PM
 * To change this template use File | Settings | File Templates.
 */

public class OpsIdentifier {
    HashMap<String, Integer> ops;
    ArrayList<String> reverse;

    public OpsIdentifier() {
        ops = new HashMap<String, Integer>();
        reverse = new ArrayList<String>();
    }

    public int getOpCode(String op) {
        Integer result;
        if ( (result = ops.get(op)) != null){
            return result;
        } else {
            reverse.add(op);
            ops.put(op, ops.size());
            return ops.size()-1;
        }
    }

    public int getRWType(String op) {
        if (op.equalsIgnoreCase("delete") ||
            op.equalsIgnoreCase("create") ||
            op.equalsIgnoreCase("setPermission") ||
            op.equalsIgnoreCase("rename")) {
            return 1;
        }
        return 0;
    }

    public String getReverse(int opcode) {
        return reverse.get(opcode);
    }
}

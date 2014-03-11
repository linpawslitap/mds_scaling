/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/19/13
 * Time: 3:53 PM
 * To change this template use File | Settings | File Templates.
 */

import java.util.ArrayList;
import java.util.Set;

public class OpsProcessor implements Processor {

    ArrayList<Integer> count;
    ArrayList<Histogram> result_list;
    OpsIdentifier op_id;

    public OpsProcessor(OpsIdentifier op_id) {
        count = new ArrayList<Integer>();
        result_list = new ArrayList<Histogram>();
        this.op_id = op_id;
    }

    public void startWindow() {
        count.clear();
    }

    public void process(Operation op,
                        Set<String> prev_window,
                        Set<String> curr_window) {
        while (op.cmd >= count.size()) {
            count.add(0);
        }
        count.set(op.cmd, count.get(op.cmd)+1);
    }

    public void finishWindow() {
        Float[] mycount = new Float[count.size()];
        float sum = 0;
        for (int i = 0; i < count.size(); ++i)
            sum += count.get(i);
        for (int i = 0; i < count.size(); ++i)
            mycount[i] = (Float) (count.get(i) / sum);

        while (result_list.size() < mycount.length)
            result_list.add(new Histogram());
        for (int i = 0; i < count.size(); ++i)
            result_list.get(i).add(mycount[i]);
    }

    public void print() {
        int opcode = 0;
        for (Histogram rate_list : result_list) {
            System.out.println("Rate of op["+op_id.getReverse(opcode)+"]");
            System.out.println(rate_list);
            ++opcode;
        }
    }

}

/*
class OpsStatistics implements Statistics {

    Float[] mycount;

    public OpsStatistics(ArrayList<Integer> count) {
        mycount = new Float[count.size()];
        float sum = 0;
        for (int i = 0; i < count.size(); ++i)
            sum += count.get(i);
        for (int i = 0; i < count.size(); ++i)
            mycount[i] = (Float) (count.get(i) / sum);
    }

    public void sum(Statistics other) {
        if (other instanceof OpsStatistics) {
            OpsStatistics true_other = (OpsStatistics) other;
            if (true_other.mycount.length > mycount.length) {
                Long[] tmp_count = new Long[true_other.mycount.length];
                for (int i = 0; i < mycount.length; ++i)
                    tmp_count[i] = mycount[i] + true_other.mycount[i];
                for (int i = mycount.length; i < true_other.mycount.length; ++i)
                    tmp_count[i] = true_other.mycount[i];
                mycount = tmp_count;
            } else {
                for (int i = 0; i < Math.min(mycount.length, true_other.mycount.length); ++i)
                    mycount[i] += true_other.mycount[i];
            }
        }
    }
}
*/

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/19/13
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */

public interface Processor {

    public void startWindow();

    public void process(Operation op,
                        Set<String> prev_window,
                        Set<String> curr_window);

    public void finishWindow();

    public void print();
}

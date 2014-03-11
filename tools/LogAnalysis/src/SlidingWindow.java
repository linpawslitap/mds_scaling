/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/19/13
 * Time: 3:26 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class SlidingWindow {

    TreeSet<String> last_window;
    TreeSet<String> curr_window;
    long window_interval;
    long curr_time;
    ArrayList<Processor> processors;
    OpsIdentifier op_id;

    SlidingWindow(long start_time, long interval) {
        curr_time = start_time;
        window_interval = interval;
        last_window = new TreeSet<String>();
        curr_window = new TreeSet<String>();

        op_id= new OpsIdentifier();
        processors = new ArrayList<Processor>();
        processors.add(new OpsProcessor(op_id));
        processors.add(new PathProcessor(interval));
        processors.add(new ConflictProcessor(interval));
    }

    void switchWindow(long new_time) {
        if (new_time - curr_time > window_interval * 2) {
            last_window.clear();
            curr_window.clear();
            curr_time = new_time;
        } else {
            curr_time = curr_time + window_interval;
            TreeSet<String> tmp;
            tmp = last_window;
            last_window = curr_window;
            curr_window = tmp;
            curr_window.clear();
        }
        for (int i = 0; i < processors.size(); ++i) {
            processors.get(i).finishWindow();
            processors.get(i).startWindow();
        }
    }

    void feed(Operation new_op) {
        if (new_op.time - curr_time > window_interval) {
            switchWindow(new_op.time);
        }
        for (Processor pi : processors)
            pi.process(new_op, last_window, curr_window);
    }

    void print() {
        for (Processor pi : processors) {
            pi.print();
        }
    }

    public static SimpleDateFormat format = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss,S");


    BufferedReader openFile(String filename) {
        BufferedReader reader = null;
        try {
            if (filename.endsWith("bz")) {
                CompressorInputStream input =
                        new CompressorStreamFactory().createCompressorInputStream(
                            new BufferedInputStream(new FileInputStream(filename)));
                reader = new BufferedReader(new InputStreamReader(input));
            } else {
                reader = new BufferedReader(new FileReader(filename));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (CompressorException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return reader;
    }

    void parseFile(String filename) {
        BufferedReader reader = openFile(filename);
        if (reader == null)
            return;
        try {
            String line;
            long count = 0;
            while ((line = reader.readLine()) != null) {
                count += 1;
                if (count % 1000000 == 0) {
                    System.out.println(count);
                    print();
                }
                String[] parts = line.split("\t");
                long start_time = format.parse(parts[0]).getTime();
                int op_code = op_id.getOpCode(parts[1]);
                int rw_type = op_id.getRWType(parts[1]);
                String src = (parts[2].equals("null")) ? null : parts[2];
                String dst = (parts[3].equals("null")) ? null : parts[3];
                Operation new_op = new Operation(start_time, op_code, rw_type, src, dst);
                feed(new_op);

                if (rw_type == 1) {
                  curr_window.add(new_op.src);
                  if (new_op.dst != null)
                    curr_window.add(new_op.dst);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        switchWindow(Long.MAX_VALUE);
        print();
    }

    public static void main(String args[]) {
        SlidingWindow analyzer = new SlidingWindow(0, 100000);
        analyzer.parseFile(args[0]);
    }
}

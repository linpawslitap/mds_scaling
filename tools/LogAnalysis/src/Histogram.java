public class Histogram {

    int num_bins;
    int count[];
    double mean;
    double tot;

    public Histogram() {
        num_bins = 101;
        count = new int[num_bins];
        mean = 0;
        tot = 0;
    }

    void add(double rate) {
        int index = (int) ((num_bins-1) * rate);
        if (index < num_bins && index >= 0)
            count[index] += 1;
        mean += rate;
        tot += 1;
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < num_bins; ++i) {
            buf.append(count[i]);
            buf.append(" ");
        }
        buf.append("\nMean:");
        buf.append(mean / tot);
        return buf.toString();
    }

}

/**
 * Created with IntelliJ IDEA.
 * User: kair
 * Date: 11/20/13
 * Time: 2:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class Histogram2D {

    int kNumBuckets;
    double kBucketLimit[];
    long count[];
    double mean;
    double tot;

    public Histogram2D() {
        kNumBuckets = 100;
        count = new long[kNumBuckets];
        mean = 0;
        tot = 0;
    }

    int getBucket(double key) {
        int lo = 0;
        int hi = kNumBuckets - 1;
        while (lo < hi) {
            int mid = lo + (hi - lo + 1) / 2;
            if  (key < kBucketLimit[mid]) hi = mid - 1;
            else if (key > kBucketLimit[mid]) lo = mid;
            else return mid;
        }
        if (lo == hi) {
            return lo;
        }
        return kNumBuckets;
    }

    void add(double rate) {

    }

    public String toString() {
        StringBuffer buf = new StringBuffer();

        buf.append("\nMean:");
        buf.append(mean / tot);
        return buf.toString();
    }

}

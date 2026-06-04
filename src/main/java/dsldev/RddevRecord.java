package dsldev;

public class RddevRecord {

    private final String day;
    private final double ddev;
    private final double rddev;

    public RddevRecord(String day, double ddev, double rddev) {
        this.day = day;
        this.ddev = ddev;
        this.rddev = rddev;
    }

    public String getDay() {
        return day;
    }

    public double getDdev() {
        return ddev;
    }

    public double getRddev() {
        return rddev;
    }
}

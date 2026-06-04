package dsldev;

import org.apache.hadoop.fs.Stat;

public class StationDayKey {

    String station;
    String day;

    public StationDayKey(String station, String day)
    {
        this.station=station;
        this.day=day;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getCombination(String c)
    {
        return station+day;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) return true;
        if (!(obj instanceof  StationDayKey)) return false;

        StationDayKey other = (StationDayKey) obj;
        return station.equals(other.station) && day.equals(other.day);
    }

    @Override
    public int hashCode()
    {
        int result = station.hashCode();
        return 31*result+day.hashCode();
    }
}

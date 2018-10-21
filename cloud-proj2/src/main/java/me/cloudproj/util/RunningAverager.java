package me.cloudproj.util;

/**
 * Created by mrleit on 4/06/17.
 */
public class RunningAverager {

    private int count = 0;
    private volatile Double currentAverage = 0.0;

    public Double newAverage(Double newValue) {
        Double total = currentAverage * count;
        currentAverage = (total + newValue) / ++count;
        return currentAverage;
    }

    public Double getCurrentAverage() {
        return Math.round(currentAverage * 100.0) / 100.0;
    }

    @Override
    public String toString() {
        return String.format("%.2f", currentAverage);
    }

    public static RunningAverager newAverager() {
        return new RunningAverager();
    }

}

package org.apache.hudi.index.radixspl;

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Objects;


public class SplineLine implements Serializable {
    private final SplinePoint start;
    private final SplinePoint end;
    private final double slope;
    private final double intercept;

    public SplineLine(SplinePoint start, SplinePoint end) {
        ValidationUtils.checkArgument(start.getKey() <= end.getKey(),
                "Spline start key must be <= end key");
        this.start = start;
        this.end = end;
        if (start.getKey() == end.getKey()) {
            this.slope = 0.0;
            this.intercept = start.getOffset();
        } else {
            this.slope = (double) (end.getOffset() - start.getOffset())
                    / (end.getKey() - start.getKey());
            this.intercept = start.getOffset() - slope * start.getKey();
        }
    }

    public double calculateY(long key) {
        if (key <= start.getKey()) {
            return start.getOffset();
        }
        if (key >= end.getKey()) {
            return end.getOffset();
        }
        double y = slope * key + intercept;
        return Math.max(start.getOffset(), Math.min(end.getOffset(), y));
    }

    public SplinePoint getStart() {
        return start;
    }

    public SplinePoint getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplineLine)) return false;
        SplineLine other = (SplineLine) o;
        return Objects.equals(start, other.start)
                && Objects.equals(end, other.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end);
    }

    @Override
    public String toString() {
        return String.format("SplineLine{start=%s, end=%s, slope=%.6f}", start, end, slope);
    }
}

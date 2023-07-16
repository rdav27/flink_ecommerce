package uv;

public class UvCount{

    private Long windowEnd;

    private Integer count;

    public UvCount(Long windowEnd, Integer count) {
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "UvCount{" +
                "windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}

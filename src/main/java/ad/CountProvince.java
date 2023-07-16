package ad;

public class CountProvince {
    private String winEnd;
    private String province;
    private Long count;

    public CountProvince(String winEnd, String province, Long count) {
        this.winEnd = winEnd;
        this.province = province;
        this.count = count;
    }

    public String getWinEnd() {
        return winEnd;
    }

    public void setWinEnd(String winEnd) {
        this.winEnd = winEnd;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CountProvince{" +
                "winEnd='" + winEnd + '\'' +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }
}

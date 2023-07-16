package ad;

public class BlackWarning {
    private Long userId;
    private Long adId;
    private String msg;

    public BlackWarning(Long userId, Long adId, String msg) {
        this.userId = userId;
        this.adId = adId;
        this.msg = msg;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "BlackWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", msg='" + msg + '\'' +
                '}';
    }
}

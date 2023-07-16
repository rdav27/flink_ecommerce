package behavior;

public class ItemViewCount {
    private Long itemId;
    private Long window;
    private Long count;

    public ItemViewCount(Long itemId, Long window, Long count) {
        this.itemId = itemId;
        this.window = window;
        this.count = count;
    }

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
    }

    public Long getWindow() {
        return window;
    }

    public void setWindow(Long window) {
        this.window = window;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId=" + itemId +
                ", window=" + window +
                ", count=" + count +
                '}';
    }
}

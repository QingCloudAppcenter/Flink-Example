package examples.bean;


public class Order {
    private String bidtime;
    private Double price;
    private String supplier;

    public String getBidtime() {
        return bidtime;
    }

    public void setBidtime(String bidtime) {
        this.bidtime = bidtime;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSupplier() {
        return supplier;
    }

    public void setSupplier(String supplier) {
        this.supplier = supplier;
    }
}

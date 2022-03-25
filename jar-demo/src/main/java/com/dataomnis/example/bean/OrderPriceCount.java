package com.dataomnis.example.bean;

public class OrderPriceCount {
    private Double price;
    private String supplier;
    private String stt;
    private String edt;

    public OrderPriceCount(Double price, String supplier, String stt, String edt) {
        this.price = price;
        this.supplier = supplier;
        this.stt = stt;
        this.edt = edt;
    }

    public OrderPriceCount() {
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

    public String getStt() {
        if (stt.contains(".")) {
            stt = stt.substring(0, stt.lastIndexOf("."));
        }
        return stt;
    }

    public void setStt(String stt) {
        this.stt = stt;
    }

    public String getEdt() {
        if (edt.contains(".")) {
            edt = edt.substring(0, edt.lastIndexOf("."));
        }
        return edt;
    }

    public void setEdt(String edt) {
        this.edt = edt;
    }

    @Override
    public String toString() {
        return "OrderPriceCount{" +
                "price=" + price +
                ", supplier='" + supplier + '\'' +
                ", stt='" + stt + '\'' +
                ", edt='" + edt + '\'' +
                '}';
    }
}

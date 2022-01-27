package examples.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderPriceCount {
    private Double price;
    private String supplier;
    private String stt;
    private String edt;
}

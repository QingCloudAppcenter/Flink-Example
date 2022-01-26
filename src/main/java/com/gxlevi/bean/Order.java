package com.gxlevi.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@AllArgsConstructor
@Getter
@ToString
public class Order {
    private String bidtime;
    private Double price;
    private String supplier;
}

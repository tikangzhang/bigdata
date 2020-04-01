package com.laozhang.entity;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Product {
    private String ip;
    private long logTime;
    private int cnt;
}

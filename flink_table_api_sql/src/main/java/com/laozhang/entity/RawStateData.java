package com.laozhang.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
public class RawStateData {
    private String state;
    private String ip;
    private String id;
    private long time;
    private String aut;
    private String run;
    private String alarm;
    private String mp;
    private String sp;
    private int cnt;
    private String[] an;
    private String[] am;
    private int door;
    private int[] tml;
    private int[] tul;

    public static void main(String[] args) {
        System.out.println(new RawStateData().toString());
    }
}

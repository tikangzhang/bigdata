package com.laozhang.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode
@ToString
public class AlarmDetail {
    private long time;
    private String ip;
    private String id;
    private String alarm;
    private String alarmMsg;
    private int door;
    private int state;
}

package com.flink.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.Timestamp;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UrlViewCount {
    public String url;
    public Long count;
    public Timestamp windowStart;
    public Timestamp windowEnd;
}

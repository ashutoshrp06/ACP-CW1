package uk.ac.ed.inf.AcpCW1.dto;

import lombok.Data;

@Data
public class SplitterRequest {
    private String readQueue;
    private String writeTopicOdd;
    private String redisHashOdd;
    private String writeTopicEven;
    private String redisHashEven;
    private int messageCount;
}
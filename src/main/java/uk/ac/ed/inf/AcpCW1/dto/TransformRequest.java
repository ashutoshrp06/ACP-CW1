package uk.ac.ed.inf.AcpCW1.dto;

import lombok.Data;

@Data
public class TransformRequest {
    private String readQueue;
    private String writeQueue;
    private int messageCount;
}
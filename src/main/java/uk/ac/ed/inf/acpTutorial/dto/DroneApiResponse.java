package uk.ac.ed.inf.acpTutorial.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DroneApiResponse {

    private String name;
    private String id;
    private Capability capability;
    private double costPer100Moves;

    public DroneApiResponse() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Capability getCapability() { return capability; }
    public void setCapability(Capability capability) { this.capability = capability; }

    public double getCostPer100Moves() { return costPer100Moves; }
    public void setCostPer100Moves(double costPer100Moves) { this.costPer100Moves = costPer100Moves; }
}
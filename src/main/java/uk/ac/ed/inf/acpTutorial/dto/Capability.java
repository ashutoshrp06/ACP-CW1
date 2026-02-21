package uk.ac.ed.inf.acpTutorial.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Capability {

    private boolean cooling;
    private boolean heating;
    private double capacity;
    private int maxMoves;
    private double costPerMove;
    private double costInitial;
    private double costFinal;

    public Capability() {}

    public boolean isCooling() { return cooling; }
    public void setCooling(boolean cooling) { this.cooling = cooling; }

    public boolean isHeating() { return heating; }
    public void setHeating(boolean heating) { this.heating = heating; }

    public double getCapacity() { return capacity; }
    public void setCapacity(double capacity) { this.capacity = capacity; }

    public int getMaxMoves() { return maxMoves; }
    public void setMaxMoves(int maxMoves) { this.maxMoves = maxMoves; }

    public double getCostPerMove() { return costPerMove; }
    public void setCostPerMove(double costPerMove) { this.costPerMove = costPerMove; }

    public double getCostInitial() { return costInitial; }
    public void setCostInitial(double costInitial) { this.costInitial = costInitial; }

    public double getCostFinal() { return costFinal; }
    public void setCostFinal(double costFinal) { this.costFinal = costFinal; }
}
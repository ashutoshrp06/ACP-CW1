package uk.ac.ed.inf.acpTutorial.dto;

import lombok.Data;

@Data
public class Tuple {
    private String item1;
    private String item2;

    public Tuple(){

    }

    public Tuple(String item1, String item2){
        this.item1 = item1;
        this.item2 = item2;
    }

    /**
     * make usage in Console.out easier
     * @return
     */
    @Override
    public String toString() {
        return "item1: " + getItem1() + " -- item2: " + getItem2();
    }
}

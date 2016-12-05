package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Envelope;

import java.io.Serializable;

/**
 * Created by ajothomas on 12/2/16.
 */
public class NewEnvelope implements Serializable{
    Envelope envelope;
    int dateStep;
    int numberPickups;

    public NewEnvelope(){
        int numberPickups = 0;
    }
    public void setEnvelope(Envelope envelope){
        this.envelope = envelope;
    }
    public void setDateStep(int dateStep){
        this.dateStep = dateStep;
    }
    public void setNumberPickups(int numberPickups){
        this.numberPickups = numberPickups;
    }

    public Envelope getEnvelope(){
        return this.envelope;
    }
    public int getDateStep(){
        return this.dateStep;
    }
    public int getNumberPickups(){
        return this.numberPickups;
    }

    @Override
    public String toString(){
        return "Date: "+dateStep+", Envelope: "+envelope+ ", Number of pickups: "+numberPickups;
    }
}

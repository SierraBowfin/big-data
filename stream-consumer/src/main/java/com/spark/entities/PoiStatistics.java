package com.spark.entities;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PoiStatistics{

    private Date startDate;
    private Date endDate;
    private Double junction_mean;    
    private Double junction_var;
    private Long junction_count;
    private Double trafficSignal_mean;
    private Double trafficSignal_var;
    private Long trafficSignal_count;
    private Double roundabout_mean;
    private Double roundabout_var;
    private Long roundabout_count;

    public PoiStatistics(
        Date startDate,
        Date endDate,
        Double junction_mean,
        Double junction_var,
        Long junction_count,
        Double trafficSignal_mean,
        Double trafficSignal_var,
        Long trafficSignal_count,
        Double roundabout_mean,
        Double roundabout_var,
        Long roundabout_count
    ){
        this.startDate = startDate;
        this.endDate = endDate;
        this.junction_mean = junction_mean;
        this.trafficSignal_mean = trafficSignal_mean;
        this.roundabout_mean = roundabout_mean;
        this.junction_var = junction_var;
        this.trafficSignal_var = trafficSignal_var;
        this.roundabout_var = roundabout_var;
        this.junction_count = junction_count;
        this.trafficSignal_count = trafficSignal_count;
        this.roundabout_count = roundabout_count;
        }

    public Date getStartDate(){
        return startDate;
    }

    public Date getEndDate(){
        return endDate;
    }

    public Double getJunction_mean(){
        return junction_mean;
    }

    public Double getTrafficSignal_mean(){
        return trafficSignal_mean;
    }

    public Double getRoundabout_mean(){
        return roundabout_mean;
    }

    public Double getJunction_var(){
        return junction_var;
    }

    public Double getTrafficSignal_var(){
        return trafficSignal_var;
    }

    public Double getRoundabout_var(){
        return roundabout_var;
    }

    public Long getJunction_count(){
        return junction_count;
    }

    public Long getTrafficSignal_count(){
        return trafficSignal_count;
    }

    public Long getRoundabout_count(){
        return roundabout_count;
    }

    public void setStartDate(Date new_start_date){
        this.startDate = new_start_date;
    }

    public void setEndDate(Date endDate){
        this.endDate = endDate;
    }

    public void setJunction_mean(Double junctionMean){
        this.junction_mean = junctionMean;
    }

    public void setTrafficSignal_mean(Double trafficSignalMean){
        this.trafficSignal_mean = trafficSignalMean;
    }

    public void setRoundabout_mean(Double roundaboutMean){
        this.roundabout_mean = roundaboutMean;
    }

    public void setJunction_var(Double junctionVar){
        this.junction_var = junctionVar;
    }

    public void setTrafficSignal_var(Double trafficSignalVar){
        this.trafficSignal_var = trafficSignalVar;
    }

    public void setRoundabout_var(Double roundaboutVar){
        this.roundabout_var = roundaboutVar;
    }

    public void setJunction_count(Long junction_count){
        this.junction_count = junction_count;
    }

    public void setTrafficSignal_count(Long trafficSignal_count){
        this.trafficSignal_count = trafficSignal_count;
    }

    public void setRoundabout_count(Long roundabout_count){
        this.roundabout_count = roundabout_count;
    }

    @Override
    public String toString() {
        SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String start = formatter.format(startDate);
        String end = formatter.format(endDate);
        return String.format("start: %s\nend: %s\nStats\tmean\tvar\tcount\nJunction\t%s\t%s\t%s\nTrafficS\t%s\t%s\t%s\nRound\t%s\t%s\t%s",
                            junction_mean, junction_var, junction_count,
                            trafficSignal_mean, trafficSignal_var, trafficSignal_count,
                            roundabout_mean, roundabout_var, roundabout_count);
    }
}
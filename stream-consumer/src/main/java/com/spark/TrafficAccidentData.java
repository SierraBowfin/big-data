package com.spark;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class TrafficAccidentData implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = -8808172610346770608L;
    
    private String id;
    private String severity;
    private Date startTime;
    private Date endTime;
    private String startLat;
    private String startLng;
    private String endLat;
    private String endLng;
    private String distanceInMiles;
    private String description;
    private String number;
    private String street;
    private String side;
    private String city;
    private String county;
    private String state;
    private String zipcode;
    private String country;
    private String timezone;
    private String airportCode;
    private Date weatherTimestamp;
    private String teperatureF;
    private String windChillF;
    private String humidityPer;
    private String pressure;
    private String visibility;
    private String windDirection;
    private String windSpeed;
    private String precipitation;
    private String weatherCondition;
    private Long amenity;
    private Long bump;
    private Long crossing;
    private Long giveWay;
    private Long junction;
    private Long noExit;
    private Long railway;
    private Long roundabout;
    private Long station;
    private Long stop;
    private Long trafficCalming;
    private Long trafficSignal;
    private Long turningLoop;
    private String sunriseSunset;
    private String civilTwilight;
    private String nauticalTwilight;
    private String astronomicalTwilight;
    private Long duration;

    private static final String dateFormatString = "yyyy-MM-dd HH:mm:ss";

    public static TrafficAccidentData createTrafficAccidentFromLine(String line) throws java.text.ParseException {
        String[] data = line.split(",");
        return new TrafficAccidentData(data);
    }

    public TrafficAccidentData(String[] data) throws java.text.ParseException{
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormatString);

        id = data[0];
        severity = data[1];
        startTime = sdf.parse(data[2]);
        endTime = sdf.parse(data[3]);
        startLat = data[4];
        startLng = data[5];
        endLat = data[6];
        endLng = data[7];
        distanceInMiles = data[8];
        description = data[9];
        number = data[10];
        street = data[11];
        side = data[12];
        city = data[13];
        county = data[14];
        state = data[15];
        zipcode = data[16];
        country = data[17];
        timezone = data[18];
        airportCode = data[19];
        weatherTimestamp = sdf.parse(data[20]);
        teperatureF = data[21];
        windChillF = data[22];
        humidityPer = data[23];
        pressure = data[24];
        visibility = data[25];
        windDirection = data[26];
        windSpeed = data[27];
        precipitation = data[28];
        weatherCondition = data[29];
        amenity = Boolean.parseBoolean(data[30]) ? 1l : 0l;
        bump = Boolean.parseBoolean(data[31]) ? 1l : 0l;
        crossing = Boolean.parseBoolean(data[32]) ? 1l : 0l;
        giveWay = Boolean.parseBoolean(data[33]) ? 1l : 0l;
        junction = Boolean.parseBoolean(data[34]) ? 1l : 0l;
        noExit = Boolean.parseBoolean(data[35]) ? 1l : 0l;
        railway = Boolean.parseBoolean(data[36]) ? 1l : 0l;
        roundabout = Boolean.parseBoolean(data[37]) ? 1l : 0l;
        station = Boolean.parseBoolean(data[38]) ? 1l : 0l;
        stop = Boolean.parseBoolean(data[39]) ? 1l : 0l;
        trafficCalming = Boolean.parseBoolean(data[40]) ? 1l : 0l;
        trafficSignal = Boolean.parseBoolean(data[41]) ? 1l : 0l;
        turningLoop = Boolean.parseBoolean(data[42]) ? 1l : 0l;
        sunriseSunset = data[43];
        civilTwilight = data[44];
        nauticalTwilight = data[45];
        astronomicalTwilight = data[46];
        duration = calculateDuration(startTime, endTime);
    }

    @Override
    public String toString() {
        return String.format(
            "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", 
            id, severity, startTime, endTime, startLat, startLng, endLat, endLng, 
            distanceInMiles, description, number, street, side, city, county, 
            state, zipcode, country, timezone, airportCode, weatherTimestamp,
            teperatureF, windChillF, humidityPer, pressure, visibility, windDirection,
            windSpeed, precipitation, weatherCondition, amenity, bump, crossing, 
            giveWay, junction, noExit, railway, roundabout, station, stop, trafficCalming, 
            trafficSignal, turningLoop, sunriseSunset, civilTwilight, nauticalTwilight,
            astronomicalTwilight,  duration
        );
    }

    public static long calculateDuration(Date start, Date end) {
        return Duration
            .between(start.toInstant(), end.toInstant())
            .toMillis();
    }

    public Long getDuration() { return duration; }
    public String getId() { return id; }
    public String getSeverity() { return severity; }
    public Date getStartTime() { return startTime; }
    public Date getEndTime() { return endTime; }
    public String getStartLat() { return startLat; }
    public String getStartLng() { return startLng; }
    public String getEndLat() { return endLat; }
    public String getEndLng() { return endLng; }
    public String getDistanceInMiles() { return distanceInMiles; }
    public String getDescription() { return description; }
    public String getNumber() { return number; }
    public String getStreet() { return street; }
    public String getSide() { return side; }
    public String getCity() { return city; }
    public String getCounty() { return county; }
    public String getState() { return state; }
    public String getZipcode() { return zipcode; }
    public String getCountry() { return country; }
    public String getTimezone() { return timezone; }
    public String getAirportCode() { return airportCode; }
    public Date getWeatherTimestamp() { return weatherTimestamp; }
    public String getTeperatureF() { return teperatureF; }
    public String getWindChillF() { return windChillF; }
    public String getHumidityPer() { return humidityPer; }
    public String getPressure() { return pressure; }
    public String getVisibility() { return visibility; }
    public String getWindDirection() { return windDirection; }
    public String getWindSpeed() { return windSpeed; }
    public String getPrecipitation() { return precipitation; }
    public String getWeatherCondition() { return weatherCondition; }
    public Long getAmenity() { return amenity; }
    public Long getBump() { return bump; }
    public Long getCrossing() { return crossing; }
    public Long getGiveWay() { return giveWay; }
    public Long getJunction() { return junction; }
    public Long getNoExit() { return noExit; }
    public Long getRailway() { return railway; }
    public Long getRoundabout() { return roundabout; }
    public Long getStation() { return station; }
    public Long getStop() { return stop; }
    public Long getTrafficCalming() { return trafficCalming; }
    public Long getTrafficSignal() { return trafficSignal; }
    public Long getTurningLoop() { return turningLoop; }
    public String getSunriseSunset() { return sunriseSunset; }
    public String getCivilTwilight() { return civilTwilight; }
    public String getNauticalTwilight() { return nauticalTwilight; }
    public String getAstronomicalTwilight() { return astronomicalTwilight; }
}
package org.neu.mapper;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neu.data.FlightCompositeKey;

import java.io.IOException;

public class FlightCountMapper extends Mapper<Object, Text, FlightCompositeKey, Object> {

    private CSVParser csvParser = new CSVParser();

    public void map(Object offset, Text value, Context context)
            throws IOException, InterruptedException {

        String[] flightRecord = this.csvParser.parseLine(value.toString());

        if (flightRecord.length > 0 && isValidRecord(flightRecord)) {

        }

    }

    private boolean isValidRecord(String[] record) {
        String crsArrTime = record[40];
        String crsDepTime = record[29];
        String crsElapsedTime = record[50];
        String airportID = record[11];
        String airportSeqID = record[12];
        String cityMarketID = record[22];
        String stateFips = record[26];
        String wac = record[28];
        String origin = record[14];
        String destination = record[23];
        String cityName = record[24];
        String state = record[25];
        String stateName = record[27];

        if (isInvalidArrDepTime(crsArrTime, crsDepTime) ||
                isInvalidTimeZone(crsArrTime, crsDepTime, crsElapsedTime) ||
                isPositive(airportID, airportSeqID, cityMarketID, stateFips, wac) ||
                isNotEmpty(origin,destination,cityName,state,stateName)
                ) {
            return false;
        }
        return true;
    }

    /**
     * @param origin
     * @param destination
     * @param cityName
     * @param state
     * @param stateName
     * @return Checks whether all above parameters are not empty. Returns true if not empty, false otherwise
     */
    private boolean isNotEmpty(String origin, String destination, String cityName, String state, String stateName) {
        return (!origin.isEmpty() && !destination.isEmpty() && !cityName.isEmpty() &&
                !state.isEmpty() && !stateName.isEmpty());
    }

    /**
     * @param airportID
     * @param airportSeqID
     * @param cityMarketID
     * @param stateFips
     * @param wac
     * @return true if all above parameters are greater than 0, false otherwise
     */
    private boolean isPositive(String airportID, String airportSeqID, String cityMarketID, String stateFips, String wac) {
        if (airportID.isEmpty() || airportSeqID.isEmpty() || cityMarketID.isEmpty() ||
                stateFips.isEmpty() || wac.isEmpty()) {
            return false;
        }
        if (Integer.parseInt(airportID) <= 0 || Integer.parseInt(airportSeqID) <= 0 ||
                Integer.parseInt(cityMarketID) <= 0 || Integer.parseInt(stateFips) <= 0 ||
                Integer.parseInt(wac) <= 0) {
            return false;
        }
        return true;
    }

    /**
     * @param crsArrTime
     * @param crsDepTime
     * @param crsElapsedTime
     * @return Calculates timezone and checks if timezone mod 60 is zero. If it is zero returns true, false otherwise.
     */
    private boolean isInvalidTimeZone(String crsArrTime, String crsDepTime, String crsElapsedTime) {
        if (crsArrTime.isEmpty() || crsDepTime.isEmpty() || crsElapsedTime.isEmpty()) return false;
        int timeZone = Integer.parseInt(crsArrTime) - Integer.parseInt(crsDepTime) - Integer.parseInt(crsElapsedTime);
        return (timeZone % 60) == 0;
    }

    /**
     *
     * @param crsArrTime
     * @param crsDepTime
     * @return Tru if both arrival and departure are not zero, false otherwise
     */
    private boolean isInvalidArrDepTime(String crsArrTime, String crsDepTime) {
        if (crsArrTime.isEmpty() || crsDepTime.isEmpty()) return false;
        else return ((Integer.parseInt(crsArrTime) != 0) && (Integer.parseInt(crsDepTime) != 0));
    }
}

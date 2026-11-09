package CinemaHallScheduler;
import  java.util.*;
public class Operator {
    
    public static Map<String,Seat> generateSeatForScreen(String screen){
        String seats[] = {"L1","L2","L3"};
        Map<String,Seat> seatsList = new HashMap<>();
        for(String seat: seats){
            seatsList.put(seat,new Seat(seat));
        }
        return seatsList;
    }
}

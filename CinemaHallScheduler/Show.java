package CinemaHallScheduler;
import java.util.*;

public class Show{
    Screen screen;
    public long startTimestamp;
    long endTimestamp;
    String movie;
    Map<>

    Show(Screen screen, long start,long end,String movie){
        this.screen = screen;
        this.startTimestamp= start;
        this.endTimestamp= end;
        this.movie = movie;
    }

    boolean holdSeats(long bookingId, List<String> seatIds){
        if(screen.get)
        return false;
    }
    boolean confirmSeats(long bookingId){
        return false;
    }
    void releaseSeats(long bookingId){
        return;
    }
}
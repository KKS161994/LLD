package CinemaHallScheduler;
import java.util.*;


public class Screen {

    private final String screenName;
    private final Map<String, Seat> seats;

    public Screen(String screenName, List<String> seatIds) {
        this.screenName = screenName;
        this.seats = new HashMap<>();

        for (String seatId : seatIds) {
            seats.put(seatId, new Seat(seatId));
        }
    }

    public String getScreenName() {
        return screenName;
    }

    public Set<String> getSeatIds() {
        return new HashSet<>(seats.keySet());
    }

    public Seat getSeat(String seatId) {
        return seats.get(seatId);
    }

    public Screen copy() {
        return new Screen(
            this.screenName,
            new ArrayList<>(this.seats.keySet())
        );
    }
}
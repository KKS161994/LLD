package CinemaHallScheduler;
class Seat {
    enum Status {
        AVAILABE,

        IN_BOOKING, 
        BOOKED,
    };

    String seatNumber;
    Status status;
    public Seat(String seatNumber, ){
        this.seatNumber= seatNumber;
        this.status = Status.AVAILABE;
    }
}

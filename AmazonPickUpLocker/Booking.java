package AmazonPickUpLocker;

public class Booking {
    long creationTime;
    Locker locker;
    long pinCode;

    Booking(long creationTime, Locker locker, long pincode){
        this.creationTime = creationTime;
        this.locker = locker;
        this.pinCode = pincode;
    }
}

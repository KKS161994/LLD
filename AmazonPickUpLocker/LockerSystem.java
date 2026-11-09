package AmazonPickUpLocker;

import java.util.*;
import java.util.concurrent.TimeUnit;

import AmazonPickUpLocker.Sizes.Size;;

public class LockerSystem {
    Map<Size, Set<Locker>> availMap = new HashMap<>();
    Map<Long, Booking> bookingMap = new HashMap<>();
    Map<Long, Booking> expirationMap = new HashMap<>();
    Set<Booking> expirBookings = new HashSet<>();
    
    // Create locker 
    public void initialiseLocker(Map<Size, Integer> sizeToCountMap) {
        int i = 0, id = 1;
        availMap.put(Size.SMALL, new HashSet<>());
        availMap.put(Size.MEDIUM, new HashSet<>());
        availMap.put(Size.LARGE, new HashSet<>());
        for (Map.Entry<Size, Integer> entry : sizeToCountMap.entrySet()) {
            Size size = entry.getKey();
            int count = entry.getValue();

            for (i = 0; i < count; i++) {
                availMap.get(size).add(new Locker(id++, size));
            }
        }
    }


    // 
    public int reserveLocker(Size size) {
        while (size != null) {
            if (availMap.get(size).size() == 0) {
                size = Sizes.upgradingSize(size);
                continue;
            }
            Locker locker = availMap.get(size).iterator().next();
            availMap.get(size).remove(locker);
            Booking booking = new Booking(System.currentTimeMillis(),
                    locker,
                    generateCode(locker.id()));
            bookingMap.put(booking.pinCode, booking);
            return locker.id();
        }
        return -1;
    }

    public int pickUpFromLocker(Long pickupCode) {
        // No such booking
        if(!bookingMap.containsKey(pickupCode)) return -1;
        //Get associated booking 
        Booking booking = bookingMap.get(pickupCode);
        // Remove the booking
        bookingMap.remove(pickupCode);
        
        // Expired booking should be removed and added 
        // to bookings
        if(System.currentTimeMillis()-booking.creationTime >
        TimeUnit.HOURS.toMillis(72)){
            expirBookings.add(booking);
            return -1;
        }

        // If booking was not expired add it to availabel
        availMap.get(booking.locker.size()).add(booking.locker);
        return booking.locker.id();
    }

    // Return item back to delivery guy
    public void returnItemFromLocker() {
        if(expirBookings.size() == 0) return;

        for(Booking booking: expirBookings){
            availMap.get(booking.locker.size()).add(booking.locker);
        }
    }


    public Long generateCode(int id) {
        Long code = Long.parseLong(id + "" + System.currentTimeMillis());
        while (bookingMap.containsKey(code)) {
            code = Long.parseLong(id + "" + System.currentTimeMillis());
        }
        return code;
    }

}

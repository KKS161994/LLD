package AmazonPickUpLocker;

import java.security.SecureRandom;
import java.time.*;
import java.util.*;
public class LockerSystemCorrected {
    


    enum Size {
        SMALL, MEDIUM, LARGE
    }

    enum BookingStatus {
        ACTIVE, EXPIRED, PICKED_UP, REMOVED
    }

    record Locker(int id, Size size) {}

    record PlacementResult(
            long bookingId,
            int lockerId,
            String pickupCode,
            Instant expiresAt
    ) {}

    record PickupResult(
            long bookingId,
            int lockerId
    ) {}

    static final class Booking {
        final long id;
        final String packageId;
        final String customerId;
        final Locker locker;
        final String pickupCode;
        final Instant placedAt;
        final Instant expiresAt;

        private BookingStatus status = BookingStatus.ACTIVE;

        Booking(
                long id,
                String packageId,
                String customerId,
                Locker locker,
                String pickupCode,
                Instant placedAt,
                Instant expiresAt
        ) {
            this.id = id;
            this.packageId = packageId;
            this.customerId = customerId;
            this.locker = locker;
            this.pickupCode = pickupCode;
            this.placedAt = placedAt;
            this.expiresAt = expiresAt;
        }

        BookingStatus status() {
            return status;
        }

        void markExpired() {
            if (status != BookingStatus.ACTIVE) {
                throw new IllegalStateException("Not active");
            }
            status = BookingStatus.EXPIRED;
        }

        void markPickedUp() {
            if (status != BookingStatus.ACTIVE) {
                throw new IllegalStateException("Not active");
            }
            status = BookingStatus.PICKED_UP;
        }

        void markRemoved() {
            if (status != BookingStatus.EXPIRED) {
                throw new IllegalStateException("Not expired");
            }
            status = BookingStatus.REMOVED;
        }
    }

    static final class LockerSystem {

        private static final Duration PICKUP_WINDOW =
                Duration.ofHours(72);

        private static final int MAX_CODE_ATTEMPTS = 100;

        // Your original availability design
        private final EnumMap<Size, Set<Locker>> available =
                new EnumMap<>(Size.class);

        // Active pickup codes
        private final Map<String, Booking> activeByCode =
                new HashMap<>();

        // Retain bookings after pickup / expiration
        private final Map<Long, Booking> history =
                new HashMap<>();

        private final SecureRandom random = new SecureRandom();
        private final Clock clock;

        private long nextBookingId = 1;

        LockerSystem(Map<Size, Integer> counts, Clock clock) {
            this.clock = Objects.requireNonNull(clock);

            for (Size size : Size.values()) {
                available.put(size, new LinkedHashSet<>());
            }

            int id = 1;

            for (Size size : Size.values()) {
                int count = counts.getOrDefault(size, 0);

                if (count < 0) {
                    throw new IllegalArgumentException(
                            "Negative capacity"
                    );
                }

                for (int i = 0; i < count; i++) {
                    available.get(size).add(
                            new Locker(id++, size)
                    );
                }
            }
        }

        // 1. Place package
        synchronized Optional<PlacementResult> placePackage(
                String packageId,
                String customerId,
                Size requestedSize
        ) {
            Objects.requireNonNull(packageId);
            Objects.requireNonNull(customerId);
            Objects.requireNonNull(requestedSize);

            Locker selected = null;

            // Smallest compatible available locker
            for (Size size : Size.values()) {

                if (size.ordinal() < requestedSize.ordinal()) {
                    continue;
                }

                Iterator<Locker> iterator =
                        available.get(size).iterator();

                if (iterator.hasNext()) {
                    selected = iterator.next();
                    iterator.remove();
                    break;
                }
            }

            if (selected == null) {
                return Optional.empty();
            }

            String code;

            try {
                code = generateUniqueCode();
            } catch (RuntimeException ex) {
                available.get(selected.size()).add(selected);
                throw ex;
            }

            Instant now = clock.instant();

            Booking booking = new Booking(
                    nextBookingId++,
                    packageId,
                    customerId,
                    selected,
                    code,
                    now,
                    now.plus(PICKUP_WINDOW)
            );

            activeByCode.put(code, booking);
            history.put(booking.id, booking);

            return Optional.of(new PlacementResult(
                    booking.id,
                    selected.id(),
                    code,
                    booking.expiresAt
            ));
        }

        // 2. Customer pickup
        synchronized Optional<PickupResult> pickupPackage(
                String code
        ) {
            Booking booking = activeByCode.get(code);

            if (booking == null) {
                return Optional.empty();
            }

            Instant now = clock.instant();

            if (!now.isBefore(booking.expiresAt)) {
                booking.markExpired();
                activeByCode.remove(code);
                return Optional.empty();
            }

            booking.markPickedUp();
            activeByCode.remove(code);

            available.get(booking.locker.size())
                     .add(booking.locker);

            return Optional.of(new PickupResult(
                    booking.id,
                    booking.locker.id()
            ));
        }

        // 3. Find and expire overdue bookings
        synchronized int expireDueBookings() {
            Instant now = clock.instant();
            int expired = 0;

            for (Booking booking : history.values()) {

                if (booking.status() == BookingStatus.ACTIVE
                        && !now.isBefore(booking.expiresAt)) {

                    booking.markExpired();
                    activeByCode.remove(booking.pickupCode);
                    expired++;
                }
            }

            return expired;
        }

        // 4. Find packages awaiting removal
        synchronized List<Long> expiredBookingIds() {
            expireDueBookings();

            List<Long> ids = new ArrayList<>();

            for (Booking booking : history.values()) {
                if (booking.status() == BookingStatus.EXPIRED) {
                    ids.add(booking.id);
                }
            }

            return List.copyOf(ids);
        }

        // 5. Associate removes expired package
        synchronized boolean removeExpiredPackage(long bookingId) {
            expireDueBookings();

            Booking booking = history.get(bookingId);

            if (booking == null ||
                    booking.status() != BookingStatus.EXPIRED) {
                return false;
            }

            booking.markRemoved();

            available.get(booking.locker.size())
                     .add(booking.locker);

            return true;
        }

        synchronized int availableCount(Size size) {
            return available.get(size).size();
        }

        synchronized Optional<BookingStatus> bookingStatus(
                long bookingId
        ) {
            Booking booking = history.get(bookingId);

            return booking == null
                    ? Optional.empty()
                    : Optional.of(booking.status());
        }

        // 6. Generate an unpredictable pickup code
        private String generateUniqueCode() {

            for (int i = 0; i < MAX_CODE_ATTEMPTS; i++) {

                String code = String.format(
                        "%08d",
                        random.nextInt(100_000_000)
                );

                if (!activeByCode.containsKey(code)) {
                    return code;
                }
            }

            throw new IllegalStateException(
                    "Unable to generate unique pickup code"
            );
        }
    }

    public static void main(String[] args) {

        Clock clock = Clock.fixed(
                Instant.parse("2026-10-24T10:00:00Z"),
                ZoneOffset.UTC
        );

        LockerSystem system = new LockerSystem(
                Map.of(
                        Size.SMALL, 1,
                        Size.MEDIUM, 1,
                        Size.LARGE, 1
                ),
                clock
        );

        PlacementResult p1 = system.placePackage(
                "P1",
                "C1",
                Size.MEDIUM
        ).orElseThrow();

        System.out.println("P1 locker: " + p1.lockerId());

        System.out.println(
                "Pickup: " +
                system.pickupPackage(p1.pickupCode())
        );

        System.out.println(
                "Available MEDIUM: " +
                system.availableCount(Size.MEDIUM)
        );

        System.out.println(
                "History P1: " +
                system.bookingStatus(p1.bookingId())
        );
    }
}



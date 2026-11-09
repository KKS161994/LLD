package CinemaHallScheduler;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class CinemaBookingSystem {

    enum SeatStatus {
        AVAILABLE, HELD, BOOKED
    }

    enum BookingStatus {
        HELD, CONFIRMED, EXPIRED, CANCELLED
    }

    static class Seat {
        final String number;

        Seat(String number) {
            this.number = number;
        }
    }

    static class Screen {
        final String id;
        final Map<String, Seat> seats = new HashMap<>();

        Screen(String id, List<String> seatNumbers) {
            this.id = id;
            for (String number : seatNumbers) {
                if (seats.putIfAbsent(number, new Seat(number)) != null) {
                    throw new IllegalArgumentException("Duplicate seat");
                }
            }
        }
    }

    static class Cinema {
        final Map<String, Screen> screens = new HashMap<>();

        void addScreen(Screen screen) {
            if (screens.putIfAbsent(screen.id, screen) != null) {
                throw new IllegalArgumentException("Duplicate screen");
            }
        }

        Screen getScreen(String id) {
            Screen screen = screens.get(id);
            if (screen == null) {
                throw new IllegalArgumentException("Unknown screen");
            }
            return screen;
        }
    }

    static class SeatReservation {
        SeatStatus status = SeatStatus.AVAILABLE;
        Long bookingId;
        Instant expiresAt;

        boolean availableAt(Instant now) {
            return status == SeatStatus.AVAILABLE ||
                   (status == SeatStatus.HELD &&
                    !now.isBefore(expiresAt));
        }

        void hold(long id, Instant expiry) {
            status = SeatStatus.HELD;
            bookingId = id;
            expiresAt = expiry;
        }

        void confirm() {
            status = SeatStatus.BOOKED;
            expiresAt = null;
        }

        void release() {
            status = SeatStatus.AVAILABLE;
            bookingId = null;
            expiresAt = null;
        }
    }

    static class Show {
        final long id;
        final String movieId;
        final Screen screen;
        final Instant startTime;
        final Instant endTime;
        final Map<String, SeatReservation> reservations =
                new HashMap<>();

        Show(long id, String movieId, Screen screen,
             Instant startTime, Instant endTime) {
            this.id = id;
            this.movieId = movieId;
            this.screen = screen;
            this.startTime = startTime;
            this.endTime = endTime;

            for (String seat : screen.seats.keySet()) {
                reservations.put(seat, new SeatReservation());
            }
        }
    }

    static class Booking {
        final long id;
        final long customerId;
        final long showId;
        final List<String> seats;
        final Instant expiresAt;
        BookingStatus status = BookingStatus.HELD;

        Booking(long id, long customerId, long showId,
                List<String> seats, Instant expiresAt) {
            this.id = id;
            this.customerId = customerId;
            this.showId = showId;
            this.seats = List.copyOf(seats);
            this.expiresAt = expiresAt;
        }
    }

    static class ShowService {
        private final Cinema cinema;
        private final ZoneId cinemaZone;
        private final AtomicLong ids = new AtomicLong();

        private final Map<Long, Show> showsById =
                new ConcurrentHashMap<>();

        private final Map<String, TreeMap<Instant, Show>>
                showsByScreen = new HashMap<>();

        private final Map<String, Map<LocalDate, Set<Long>>>
                showsByMovieAndDate = new HashMap<>();

        ShowService(Cinema cinema, ZoneId cinemaZone) {
            this.cinema = cinema;
            this.cinemaZone = cinemaZone;
        }

        public synchronized Show scheduleShow(
                String movieId, String screenId,
                Instant start, Instant end) {

            if (start == null || end == null ||
                !start.isBefore(end)) {
                throw new IllegalArgumentException("Invalid interval");
            }

            Screen screen = cinema.getScreen(screenId);

            TreeMap<Instant, Show> schedule =
                    showsByScreen.computeIfAbsent(
                            screenId, k -> new TreeMap<>());

            Map.Entry<Instant, Show> previous =
                    schedule.floorEntry(start);

            Map.Entry<Instant, Show> next =
                    schedule.ceilingEntry(start);

            if (previous != null &&
                previous.getValue().endTime.isAfter(start)) {
                throw new IllegalStateException("Overlapping show");
            }

            if (next != null && next.getKey().isBefore(end)) {
                throw new IllegalStateException("Overlapping show");
            }

            Show show = new Show(
                    ids.incrementAndGet(), movieId,
                    screen, start, end);

            schedule.put(start, show);

            LocalDate date =
                    start.atZone(cinemaZone).toLocalDate();

            showsByMovieAndDate
                    .computeIfAbsent(movieId, k -> new HashMap<>())
                    .computeIfAbsent(date, k -> new HashSet<>())
                    .add(show.id);

            showsById.put(show.id, show);
            return show;
        }

        public synchronized List<Show> searchShows(
                String movieId, LocalDate date) {

            Set<Long> ids = showsByMovieAndDate
                    .getOrDefault(movieId, Map.of())
                    .getOrDefault(date, Set.of());

            return ids.stream()
                    .map(showsById::get)
                    .sorted(Comparator.comparing(s -> s.startTime))
                    .toList();
        }

        public Show getShow(long id) {
            Show show = showsById.get(id);
            if (show == null) {
                throw new IllegalArgumentException("Unknown show");
            }
            return show;
        }
    }

    static class BookingService {
        private final ShowService showService;
        private final AtomicLong ids = new AtomicLong();

        private final Map<Long, Booking> bookingsById =
                new ConcurrentHashMap<>();

        BookingService(ShowService showService) {
            this.showService = showService;
        }

        public Booking holdSeats(
                long customerId, long showId,
                List<String> seatNumbers) {

            Show show = showService.getShow(showId);

            synchronized (show) {
                Instant now = Instant.now();

                if (!now.isBefore(show.startTime)) {
                    throw new IllegalStateException("Show already started");
                }

                if (seatNumbers == null || seatNumbers.isEmpty() ||
                    new HashSet<>(seatNumbers).size() != seatNumbers.size()) {
                    throw new IllegalArgumentException("Invalid seats");
                }

                for (String seat : seatNumbers) {
                    SeatReservation reservation =
                            show.reservations.get(seat);

                    if (reservation == null ||
                        !reservation.availableAt(now)) {
                        throw new IllegalStateException(
                                "Seat unavailable: " + seat);
                    }
                }

                long bookingId = ids.incrementAndGet();
                Instant expiry = now.plus(Duration.ofMinutes(5));

                Booking booking = new Booking(
                        bookingId, customerId, showId,
                        seatNumbers, expiry);

                // Register before changing seat ownership.
                bookingsById.put(bookingId, booking);

                for (String seat : seatNumbers) {
                    SeatReservation reservation =
                            show.reservations.get(seat);

                    // An expired previous hold may still exist.
                    if (reservation.bookingId != null) {
                        Booking previous =
                                bookingsById.get(reservation.bookingId);

                        if (previous != null &&
                            previous.status == BookingStatus.HELD) {
                            previous.status = BookingStatus.EXPIRED;
                        }
                    }

                    reservation.hold(bookingId, expiry);
                }

                return booking;
            }
        }

        public Booking confirmBooking(long bookingId) {
            Booking booking = getBooking(bookingId);
            Show show = showService.getShow(booking.showId);

            synchronized (show) {
                Instant now = Instant.now();

                if (booking.status == BookingStatus.CONFIRMED) {
                    return booking;
                }

                if (booking.status != BookingStatus.HELD) {
                    throw new IllegalStateException("Booking not held");
                }

                if (!now.isBefore(booking.expiresAt)) {
                    expireBookingLocked(booking, show);
                    throw new IllegalStateException("Hold expired");
                }

                if (!now.isBefore(show.startTime)) {
                    throw new IllegalStateException("Show already started");
                }

                for (String seat : booking.seats) {
                    SeatReservation reservation =
                            show.reservations.get(seat);

                    if (reservation.status != SeatStatus.HELD ||
                        !Objects.equals(reservation.bookingId, bookingId)) {
                        throw new IllegalStateException(
                                "Reservation ownership mismatch");
                    }
                }

                for (String seat : booking.seats) {
                    show.reservations.get(seat).confirm();
                }

                booking.status = BookingStatus.CONFIRMED;
                return booking;
            }
        }

        public Booking cancelBooking(long bookingId) {
            Booking booking = getBooking(bookingId);
            Show show = showService.getShow(booking.showId);

            synchronized (show) {
                if (booking.status == BookingStatus.CANCELLED) {
                    return booking;
                }

                if (booking.status == BookingStatus.EXPIRED) {
                    throw new IllegalStateException("Already expired");
                }

                if (!Instant.now().isBefore(show.startTime)) {
                    throw new IllegalStateException("Show already started");
                }

                for (String seat : booking.seats) {
                    SeatReservation reservation =
                            show.reservations.get(seat);

                    if (!Objects.equals(reservation.bookingId, bookingId)) {
                        throw new IllegalStateException(
                                "Reservation ownership mismatch");
                    }
                }

                for (String seat : booking.seats) {
                    show.reservations.get(seat).release();
                }

                booking.status = BookingStatus.CANCELLED;
                return booking;
            }
        }

        public List<String> getAvailableSeats(long showId) {
            Show show = showService.getShow(showId);

            synchronized (show) {
                Instant now = Instant.now();
                return show.reservations.entrySet().stream()
                        .filter(e -> e.getValue().availableAt(now))
                        .map(Map.Entry::getKey)
                        .sorted()
                        .toList();
            }
        }

        public void expireHolds() {
            Instant now = Instant.now();

            for (Booking booking : bookingsById.values()) {
                if (booking.status != BookingStatus.HELD ||
                    now.isBefore(booking.expiresAt)) {
                    continue;
                }

                Show show = showService.getShow(booking.showId);

                synchronized (show) {
                    if (booking.status == BookingStatus.HELD &&
                        !Instant.now().isBefore(booking.expiresAt)) {
                        expireBookingLocked(booking, show);
                    }
                }
            }
        }

        private void expireBookingLocked(Booking booking, Show show) {
            for (String seat : booking.seats) {
                SeatReservation reservation =
                        show.reservations.get(seat);

                if (Objects.equals(
                        reservation.bookingId, booking.id) &&
                    reservation.status == SeatStatus.HELD) {
                    reservation.release();
                }
            }

            booking.status = BookingStatus.EXPIRED;
        }

        private Booking getBooking(long id) {
            Booking booking = bookingsById.get(id);
            if (booking == null) {
                throw new IllegalArgumentException("Unknown booking");
            }
            return booking;
        }
    }

    public static void main(String[] args) {
        Cinema cinema = new Cinema();
        cinema.addScreen(new Screen(
                "SCREEN_1", List.of("A1", "A2", "A3", "B1")));

        ShowService shows = new ShowService(
                cinema, ZoneId.of("Asia/Kolkata"));

        Instant start = Instant.now().plus(Duration.ofHours(2));

        Show show = shows.scheduleShow(
                "MOVIE_1", "SCREEN_1",
                start, start.plus(Duration.ofHours(2)));

        BookingService bookings = new BookingService(shows);

        Booking first = bookings.holdSeats(
                101, show.id, List.of("A1", "A2"));

        System.out.println("Held: " + first.id);

        try {
            bookings.holdSeats(102, show.id, List.of("A2", "A3"));
            throw new AssertionError("Double booking allowed");
        } catch (IllegalStateException expected) {
            System.out.println("Double booking prevented");
        }

        bookings.confirmBooking(first.id);
        System.out.println("Confirmed: " + first.status);

        bookings.cancelBooking(first.id);
        System.out.println("Cancelled: " + first.status);

        Booking second = bookings.holdSeats(
                102, show.id, List.of("A1", "A2"));

        System.out.println("New booking: " + second.id);
        System.out.println(
                "Available seats: " +
                bookings.getAvailableSeats(show.id));
    }
}

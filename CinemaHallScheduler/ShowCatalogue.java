package CinemaHallScheduler;

import java.util.*;
import java.util.stream.Collectors;

public class ShowCatalogue {

    // movie -> screen -> timestamp
    Map<String, Set<Show>> movieToShow;
    ScreenCatalogue screenCatalogue;
    private final Map<String, TreeMap<Long, Show>> screenToShow = new HashMap<>();

    ShowCatalogue() {
        this.movieToShow = new HashMap<>();
        this.screenCatalogue = new ScreenCatalogue();
    }

public Set<Show> getShows(){
    return movieToShow.values()
    .stream()
    .flatMap(Set::stream)
    .collect(Collectors.toSet());
}

public synchronized boolean addShow(
        String movie,
        long startTimestamp,
        long endTimestamp,
        Screen screen
) {
    // 1. Validate time interval
    if (startTimestamp >= endTimestamp) {
        throw new IllegalArgumentException("Invalid show timing");
    }

    // 2. Get scheduled shows for this screen
    TreeMap<Long, Show> shows = screenToShow.computeIfAbsent(
        screen.screenName,
        k -> new TreeMap<>()
    );

    // 3. Find neighboring shows
    Map.Entry<Long, Show> floor = shows.floorEntry(startTimestamp);
    Map.Entry<Long, Show> ceiling = shows.ceilingEntry(startTimestamp);

    // 4. Check overlap with previous show
    if (floor != null &&
        floor.getValue().endTimestamp > startTimestamp) {
        return false;
    }

    // 5. Check overlap with next show
    if (ceiling != null &&
        ceiling.getKey() < endTimestamp) {
        return false;
    }

    // 6. Create and insert new show
    Show newShow = new Show(
        screen,
        startTimestamp,
        endTimestamp,
        movie
    );

    shows.put(startTimestamp, newShow);
    movieToShow.putIfAbsent(movie,new HashSet<Show>();
    movieToShow.get(movie).add(newShow);
    return true;
}
}

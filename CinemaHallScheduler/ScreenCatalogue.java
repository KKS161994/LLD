package CinemaHallScheduler;
import java.util.*;
public class ScreenCatalogue {
    private Map<String,Screen> screens;

    ScreenCatalogue(){
        this.screens = new HashMap<>();
    }

    public boolean addScreen(String screenName,List<String> seats){
        if(screens.containsKey(screenName)) return false;
        screens.put(screenName,Screen.newInstance(screenName,seats));
        return true;
    }

    public Screen getScreen(String screen){
        // return screens.get(screen).clone();
    } 


}

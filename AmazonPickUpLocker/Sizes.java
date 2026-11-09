package AmazonPickUpLocker;

public class Sizes {
    enum Size {
        SMALL, MEDIUM, LARGE
    }

    static Size upgradingSize(Size size) {
        if (size == Size.SMALL)
            return Size.MEDIUM;
        if (size == Size.MEDIUM)
            return Size.LARGE;
        return null;
    }

}

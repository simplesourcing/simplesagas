package io.simplesource.saga.shared.data;

import java.util.*;

// TODO: replace with Java 10 List.of
public class Lists {
    public static <E> List<E> of(E... elements) {
        return new ArrayList<>(Arrays.asList(elements));
    }
}


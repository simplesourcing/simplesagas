package io.simplesource.saga.shared.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

// TODO: replace with Java 10 Set.of
public class Sets {
    public static <E> Set<E> of(E... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }
}

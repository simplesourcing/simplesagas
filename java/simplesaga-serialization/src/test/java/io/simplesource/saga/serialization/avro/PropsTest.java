package io.simplesource.saga.serialization.avro;

import net.jqwik.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: replace all Serdes tests with property based tests
class PropsTest {
    private static Logger logger = LoggerFactory.getLogger(PropsTest.class);

    @Property
    boolean generates_integers(@ForAll("integerProvider") Integer sampleInt) {
        logger.info(sampleInt.toString());
        return true;
    }

    @Provide
    Arbitrary<Integer> integerProvider() {
        Arbitrary<Integer> length = Arbitraries.integers().between(5, 10);
        return length;
    }
}

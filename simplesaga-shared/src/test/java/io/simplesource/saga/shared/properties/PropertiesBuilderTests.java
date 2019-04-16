package io.simplesource.saga.shared.properties;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;


class PropertiesBuilderTests {
    @Test
    void testInitialAndFinalSteps() {
        PropertiesBuilder.BuildSteps userSteps = pb -> pb
                .withProperty("A", "user's value for A")
                .withProperty("B", "user's value for B");

        Properties props = userSteps
                .withInitialStep(pb ->
                        pb.withProperty("A", "framework's value for A"))
                .withNextStep(pb ->
                        pb.withProperty("B", "framework's value for B")).build();

        assertPropValues(props);
    }

    @Test
    void testMultipleAdd() {
        Map<String, Object> userProps = new HashMap<>();
        userProps.put("A", "user's value for A");
        userProps.put("B", "user's value for B");

        PropertiesBuilder.BuildSteps userSteps = pb -> pb
                .withProperties(userProps);

        Properties props = userSteps
                .withInitialStep(pb ->
                        pb.withProperty("A", "framework's value for A"))
                .withNextStep(pb ->
                        pb.withProperty("B", "framework's value for B")).build();

        assertPropValues(props);
        PropertiesBuilder.BuildSteps copySteps = pb -> pb.withProperties(props);

        assertPropValues(copySteps.build());
    }


    private void assertPropValues(Properties props) {
        assertThat(props.get("A")).isEqualTo("user's value for A");
        assertThat(props.get("B")).isEqualTo("framework's value for B");
    }

}
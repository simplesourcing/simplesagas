package io.simplesource.saga.saga.dsl;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.simplesource.data.NonEmptyList;

import static org.assertj.core.api.Assertions.assertThat;
import static io.simplesource.saga.saga.dsl.SagaDsl.*;

import io.simplesource.saga.model.saga.ActionCommand;
import io.simplesource.saga.model.saga.Saga;
import org.junit.jupiter.api.Test;

class DslTest {
    UUID randomId() {
        return UUID.randomUUID();
    }

    SagaDsl.SagaBuilder<String> builder = new SagaDsl.SagaBuilder<>();

    SagaDsl.Fragment<String> create(String a) {
        return builder.addAction(randomId(), "actionType-" + a, new ActionCommand<>(randomId(), "Command-" + a));
    }

    public void dependsOnSet(String action, Set<String> dependsOn, Saga<String> saga) {
        Set<String> z = saga.actions.values().stream().filter(x -> x.actionType.equals("actionType-" + action))
                .findFirst()
                .get()
                .dependencies
                .stream()
                .map(saga.actions::get)
                .map(x -> x.actionType).collect(Collectors.toSet());
        assertThat(z).isEqualTo(dependsOn.stream().map(x -> "actionType-" + x).collect(Collectors.toSet()));
    }

    public void dependsOn(String action, String dependsOn, Saga<String> saga) {
        dependsOnSet(action, Collections.singleton(dependsOn), saga);
    }

    @Test
    void actionDslMustCreateDependencies() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);
        assertThat(list.get(2)).isEqualTo(30);
        
        SagaDsl.Fragment<String> a1 = create("1");
        SagaDsl.Fragment<String> a2 = create("2");
        SagaDsl.Fragment<String> a3 = create("3");

        SagaDsl.Fragment<String> a4 = create("4");
        SagaDsl.Fragment<String> a5a = create("5a");
        SagaDsl.Fragment<String> a5b = create("5b");
        SagaDsl.Fragment<String> a6 = create("6");

        SagaDsl.Fragment<String> a7 = create("7");
        SagaDsl.Fragment<String> a8a = create("8a");
        SagaDsl.Fragment<String> a8b = create("8b");
        SagaDsl.Fragment<String> a9 = create("9");

        SagaDsl.Fragment<String> a10 = create("10");
        SagaDsl.Fragment<String> a11a = create("11a");
        SagaDsl.Fragment<String> a11b = create("11b");
        SagaDsl.Fragment<String> a12 = create("12");

        SagaDsl.Fragment<String> a13 = create("13");


        a1.andThen(a2).andThen(a3);

        a4.andThen(inParallel(a5a, a5b)).andThen(a6);

        a7.andThen(inSeries(a8a, a8b)).andThen(a9);

        (a10.andThen(a11a)).andThen(a12);
        a10.andThen((a11b).andThen(a12));

        a7.andThen((a13).andThen(a9));
        Saga<String> saga = builder.build().getOrElse(null);

        dependsOnSet("1", Collections.emptySet(), saga);
        dependsOn("2", "1", saga);
        dependsOn("3", "2", saga);

        dependsOnSet("4", Collections.emptySet(), saga);
        dependsOn("5a", "4", saga);
        dependsOn("5b", "4", saga);

        dependsOnSet("6", Sets.newHashSet("5a", "5b"), saga);

        dependsOnSet("7", Collections.emptySet(), saga);
        dependsOn("8a", "7", saga);
        dependsOn("8b", "8a", saga);
        dependsOnSet("9", Sets.newHashSet("8b", "13"), saga);

        dependsOnSet("10", Collections.emptySet(), saga);
        dependsOn("11a", "10", saga);
        dependsOn("11b", "10", saga);
        dependsOnSet("12", Sets.newHashSet("11a", "11b"), saga);

        dependsOn("13", "7", saga);
    }
}

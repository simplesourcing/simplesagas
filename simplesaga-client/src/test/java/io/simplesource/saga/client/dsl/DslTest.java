package io.simplesource.saga.client.dsl;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import io.simplesource.data.NonEmptyList;

import static org.assertj.core.api.Assertions.assertThat;
import static io.simplesource.saga.client.dsl.SagaDSL.*;

import io.simplesource.saga.model.action.ActionId;
import io.simplesource.saga.model.saga.Saga;
import io.simplesource.saga.shared.data.Sets;
import org.junit.jupiter.api.Test;

class DSLTest {
    SagaBuilder<String> builder = SagaBuilder.create();

    SubSaga<String> create(String a) {
        return builder.addAction(ActionId.random(), "actionType-" + a, "Command-" + a);
    }

    void dependsOnSet(String action, Set<String> dependsOn, Saga<String> saga) {
        Set<String> z = saga.actions.values().stream().filter(x -> x.command.actionType.equals("actionType-" + action))
                .findFirst()
                .get()
                .dependencies
                .stream()
                .map(saga.actions::get)
                .map(x -> x.command.actionType).collect(Collectors.toSet());
        assertThat(z).isEqualTo(dependsOn.stream().map(x -> "actionType-" + x).collect(Collectors.toSet()));
    }

    void dependsOn(String action, String dependsOn, Saga<String> saga) {
        dependsOnSet(action, Collections.singleton(dependsOn), saga);
    }

    @Test
    void actionDSLMustCreateDependencies() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);
        assertThat(list.get(2)).isEqualTo(30);

        SubSaga<String> a1 = create("1");
        SubSaga<String> a2 = create("2");
        SubSaga<String> a3 = create("3");

        SubSaga<String> a4 = create("4");
        SubSaga<String> a5a = create("5a");
        SubSaga<String> a5b = create("5b");
        SubSaga<String> a6 = create("6");

        SubSaga<String> a7 = create("7");
        SubSaga<String> a8a = create("8a");
        SubSaga<String> a8b = create("8b");
        SubSaga<String> a9 = create("9");

        SubSaga<String> a10 = create("10");
        SubSaga<String> a11a = create("11a");
        SubSaga<String> a11b = create("11b");
        SubSaga<String> a12 = create("12");

        SubSaga<String> a13 = create("13");


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

        dependsOnSet("6", Sets.of("5a", "5b"), saga);

        dependsOnSet("7", Collections.emptySet(), saga);
        dependsOn("8a", "7", saga);
        dependsOn("8b", "8a", saga);
        dependsOnSet("9", Sets.of("8b", "13"), saga);

        dependsOnSet("10", Collections.emptySet(), saga);
        dependsOn("11a", "10", saga);
        dependsOn("11b", "10", saga);
        dependsOnSet("12", Sets.of("11a", "11b"), saga);

        dependsOn("13", "7", saga);
    }

    @Test
    void emptyDependencies() {
        SubSaga<String> a0p = inParallel(Collections.emptyList());
        SubSaga<String> a0s = inSeries(Collections.emptyList());
        SubSaga<String> a1 = create("1");
        SubSaga<String> a1p = inParallel(Collections.emptyList());
        SubSaga<String> a2 = create("2");
        SubSaga<String> a2s = inSeries(Collections.emptyList());
        SubSaga<String> a3 = create("3");
        SubSaga<String> a3s = inParallel(Collections.emptyList());
        SubSaga<String> a3p = inSeries(Collections.emptyList());

        a0s.andThen(a0p)
                .andThen(a0s)
                .andThen(a1)
                .andThen(a1p)
                .andThen(a2)
                .andThen(a2s)
                .andThen(a3)
                .andThen(a3p)
                .andThen(a3s);

        Saga<String> saga = builder.build().getOrElse(null);

        assertThat(saga.actions.keySet().size()).isEqualTo(3);

        dependsOnSet("1", Collections.emptySet(), saga);
        dependsOn("2", "1", saga);
        dependsOn("3", "2", saga);
    }
}

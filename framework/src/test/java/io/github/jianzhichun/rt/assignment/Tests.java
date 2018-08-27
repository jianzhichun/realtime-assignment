package io.github.jianzhichun.rt.assignment;

import com.google.common.collect.Queues;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.functions.Functions;
import io.reactivex.internal.operators.flowable.FlowableFromObservable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author zao
 * @date 2018/08/27
 */
public class Tests {

    @Test
    public void test() {

        PublishSubject<Assignable.SimpleAssignable> subject = PublishSubject.create();
        AtomicReference<Boolean> flag = new AtomicReference<>(true);

        AssignmentManager.SimpleAssignmentManager<String, Assignable.SimpleAssignable> manager =
            new AssignmentManager.SimpleAssignmentManager<>(
                (assignables, assignees) -> Queues
                    .newLinkedBlockingDeque(assignables.stream().peek(assignable -> {
                        int num = (int)assignable.getItems().get("num");
                        if ((num & 1) == 0) {
                            assignable.setAssignee("even");
                        } else {
                            assignable.setAssignee("odd");
                        }
                    }).collect(Collectors.toList()))
            );

        AssigneeFlowableOnBackpressureBuffer<String, Assignable.SimpleAssignable> flowable =
            new AssigneeFlowableOnBackpressureBuffer<>(
                new FlowableFromObservable<>(subject),
                false,
                Functions.EMPTY_ACTION,
                manager
            );

        Disposable disposable =
            flowable
                .observeOn(Schedulers.newThread())
                .groupBy(Assignable.SimpleAssignable::getAssignee)
                .subscribe(
                    group -> {
                        switch (group.getKey()) {
                            case "odd":
                                group.map(Assignable.SimpleAssignable::getItems)
                                     .map(Map::toString)
                                     .map("odd group"::concat)
                                     .subscribe(System.out::println);
                                break;
                            default:
                                group.map(Assignable.SimpleAssignable::getItems)
                                     .map(Map::toString)
                                     .map("even group"::concat)
                                     .subscribe(System.out::println);
                                break;
                        }
                    },
                    ExceptionUtils::printRootCauseStackTrace,
                    () -> flag.set(false)
                );

        IntStream.range(0, 7).forEach(
            i -> subject.onNext(
                Assignable.SimpleAssignable.builder()
                                           .item("num", i)
                                           .build())
        );

        subject.onComplete();

        while (flag.get()) {}
        disposable.dispose();

    }
}

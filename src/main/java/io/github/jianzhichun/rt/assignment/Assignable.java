package io.github.jianzhichun.rt.assignment;

import lombok.*;

import java.util.Map;

/**
 * @author zao
 * @date 2018/08/24
 */
public interface Assignable<T> {

    /**
     * TODO
     *
     * @return T
     */
    T getAssignee();

    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    class SimpleAssignable implements Assignable<String> {

        @Singular
        private Map<String, Object> items;
        private String assignee;
    }
}

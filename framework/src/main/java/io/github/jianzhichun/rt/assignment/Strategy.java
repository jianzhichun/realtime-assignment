package io.github.jianzhichun.rt.assignment;

import java.util.Collection;
import java.util.Queue;

/**
 * @author zao
 * @date 2018/08/24
 */
public interface Strategy<ASSIGNEE, T extends Assignable<ASSIGNEE>> {

    Queue<T> reassign(Collection<T> assignables, Collection<ASSIGNEE> assignees);
}

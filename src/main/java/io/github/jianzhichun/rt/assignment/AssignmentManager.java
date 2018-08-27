package io.github.jianzhichun.rt.assignment;

import com.google.common.collect.Lists;
import io.reactivex.annotations.Nullable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import lombok.Setter;

import java.util.List;
import java.util.Objects;
import java.util.Queue;

/**
 * @author zao
 * @date 2018/08/24
 */
public interface AssignmentManager<ASSIGNEE, T extends Assignable<ASSIGNEE>>
    extends SimplePlainQueue<T> {

    /**
     * TODO
     *
     * @param assignee assignee
     */
    boolean addAssignee(ASSIGNEE assignee);

    /**
     * TODO
     *
     * @param assignee assignee
     */
    boolean removeAssignee(ASSIGNEE assignee);

    class SimpleAssignmentManager<ASSIGNEE, T extends Assignable<ASSIGNEE>>
        implements AssignmentManager<ASSIGNEE, T> {

        private List<ASSIGNEE> assignees = Lists.newCopyOnWriteArrayList();
        private final List<T> assignables = Lists.newArrayList();

        private volatile Queue<T> queue;
        @Setter
        private volatile Strategy<ASSIGNEE, T> strategy;

        public SimpleAssignmentManager(Strategy<ASSIGNEE, T> strategy) {
            this.strategy = strategy;
        }

        @Override
        public boolean addAssignee(ASSIGNEE assignee) {
            if (assignees.add(assignee)) {
                return deal();
            }
            return false;
        }

        @Override
        public boolean removeAssignee(ASSIGNEE assignee) {
            if (assignees.remove(assignee)) {
                return deal();
            }
            return false;
        }

        @Override
        public boolean offer(T value) {
            synchronized (assignables) {
                if (assignables.add(value)) {
                    return deal();
                }
                return false;
            }
        }

        @Override
        public boolean offer(T v1, T v2) {
            return offer(v1) && offer(v2);
        }

        private boolean deal() {
            queue = strategy.reassign(assignables, assignees);
            return true;
        }

        @Nullable
        @Override
        public T poll() {
            if (Objects.isNull(queue)) {
                return null;
            } else {
                T assignable = queue.poll();
                assignables.remove(assignable);
                return assignable;
            }
        }

        @Override
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        @Override
        public void clear() {
            this.assignables.clear();
        }


    }

}

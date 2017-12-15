package com.splicemachine.derby.stream.control;

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;

/**
 *
 * Iterator over Futures
 *
 *
 */
public class FutureIterator<T> implements Iterator<T> {


        @SafeVarargs
        public static <T> Iterator<T> concat(Future<Iterator<T>>... futureIterators) {
            return new FutureIterator<>(futureIterators);
        }

        private final List<Future<Iterator<T>>> futureIterators;
        private Iterator<T> current;

        @SafeVarargs
        public FutureIterator(final Future<Iterator<T>>... futureIterators) {
            this.futureIterators = new LinkedList<>(asList(futureIterators));
        }

        @Override
        public boolean hasNext() {
            checkNext();
            return current != null && current.hasNext();
        }

        @Override
        public T next() {
            checkNext();
            if (current == null || !current.hasNext()) throw new NoSuchElementException();
            return current.next();
        }

        @Override
        public void remove() {
            if (current == null) throw new IllegalStateException();
            current.remove();
        }

        private void checkNext() {
            try {
                while ((current == null || !current.hasNext()) && !futureIterators.isEmpty()) {
                    current = futureIterators.remove(0).get(); // Blocked Until Available
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

}





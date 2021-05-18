package org.apache.flink.connectors.test.common;

import org.apache.flink.connectors.test.common.testsuites.TestSuiteBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** IteratorWithCurrentTest. */
public class IteratorWithCurrentTest {

    @Test
    public void testIterator() {
        List<Integer> numberList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            numberList.add(i);
        }
        TestSuiteBase.IteratorWithCurrent<Integer> iterator =
                new TestSuiteBase.IteratorWithCurrent<>(numberList.iterator());
        Integer num = 0;
        while (iterator.hasNext()) {
            assertEquals(num, iterator.next());
            num++;
        }
        assertEquals(10, num.intValue());
        assertNull(iterator.current());
    }

    @Test
    public void testEmptyList() {
        TestSuiteBase.IteratorWithCurrent<Integer> iterator =
                new TestSuiteBase.IteratorWithCurrent<>(Collections.emptyIterator());
        assertNull(iterator.current());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testCurrentElement() {
        List<Integer> numberList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            numberList.add(i);
        }
        TestSuiteBase.IteratorWithCurrent<Integer> iterator =
                new TestSuiteBase.IteratorWithCurrent<>(numberList.iterator());
        Integer num = 0;
        while (iterator.hasNext()) {
            assertEquals(num, iterator.current());
            assertEquals(num, iterator.next());
            num++;
        }
        assertEquals(10, num.intValue());
        assertNull(iterator.current());
    }
}


package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.Collection;
import java.util.Comparator;
import java.util.Arrays;

/**
 * An unbounded priority based on a priority heap.
 * The elements of the priority queue are ordered according to their
 * {@linkplain Comparable natural ordering}, or by a {@link Comparator}
 * provided at queue construction time, depending on which constructor is
 * used.  A priority queue does not permit {@code null} elements.
 * A priority queue relying on natural ordering also does not permit
 * insertion of non-comparable objects (doing so may result in
 * {@code ClassCastException}).
 *
 * <p>The <em>head</em> of this queue is the <em>least</em> element
 * with respect to the specified ordering.  If multiple elements are
 * tied for least value, the head is one of those elements -- ties are
 * broken arbitrarily.  The queue retrieval operations {@code poll},
 * {@code remove}, {@code peek}, and {@code element} access the
 * element at the head of the queue.
 *
 *
 * <p> <strong>Note that this implementation is not synchronized.</strong>
 *
 * <p>Implementation note: It's based on Floyd's heap construction algorithm.
 * This specific implementation is optimized for yarn scheduler. It has fewer
 * comparing times than original Floyd's algorithm.
 *
 * @param <E> the type of elements held in this collection
 */
public class CuedFloydHeap<E> {
    /**
     * Priority queue represented as a balanced binary heap: the two
     * children of queue[n] are queue[2*n+1] and queue[2*(n+1)].  The
     * priority queue is ordered by comparator, or by the elements'
     * natural ordering, if comparator is null: For each node n in the
     * heap and each descendant d of n, n <= d.  The element with the
     * lowest value is in queue[0], assuming the queue is nonempty.
     */
    private transient Object[] queue;
    private byte[] rememberdOrder;

    /**
     * The number of elements in the priority queue.
     */
    private int size = 0;

    /**
     * The comparator, or null if priority queue uses elements'
     * natural ordering.
     */
    private final Comparator<? super E> comparator;

    public CuedFloydHeap(Comparator<? super E> comparator) {
        this.comparator = comparator;
    }

    private CuedFloydHeap(Object[] a,
                          Comparator<? super E> comparator) {
        this.comparator = comparator;
        this.queue = a;
        this.size = a.length;
        rememberdOrder = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            if (a[i] == null)
                throw new NullPointerException();
            rememberdOrder[i] = ORDER_UNKNOWN;
        }
        heapify();
    }

    /**
     * Creates a {@code Heap} containing the elements in the
     * specified collection.  If the specified collection is an instance of
     * a SortedSet or is another {@code Heap}, this
     * priority queue will be ordered according to the same ordering.
     * Otherwise, this priority queue will be ordered according to the
     * {@linkplain Comparable natural ordering} of its elements.
     *
     * @param  c the collection whose elements are to be placed
     *         into this priority queue
     * @throws ClassCastException if elements of the specified collection
     *         cannot be compared to one another according to the priority
     *         queue's ordering
     * @throws NullPointerException if the specified collection or any
     *         of its elements are null
     */
    public CuedFloydHeap(Collection<? extends E> c,
                         Comparator<? super E> comparator) {
        this.comparator = comparator;
        initElementsFromCollection(c);
        heapify();
    }

    private static final byte ORDER_UNKNOWN = 0;
    private static final byte ORDER_LEFT_MIN = 1;
    private static final byte ORDER_RIGHT_MIN = 2;

    public void initElementsFromCollection(Collection<? extends E> c) {
        Object[] a = c.toArray();
        // If c.toArray incorrectly doesn't return Object[], copy it.
        if (a.getClass() != Object[].class)
            a = Arrays.copyOf(a, a.length, Object[].class);
        int len = a.length;
        rememberdOrder = new byte[len];
        for (int i = 0; i < len; i++) {
            if (a[i] == null)
                throw new NullPointerException();
            rememberdOrder[i] = ORDER_UNKNOWN;
        }
        this.queue = a;
        this.size = a.length;
    }

    @SuppressWarnings("unchecked")
    public E peek() {
        if (size == 0)
            return null;
        return (E) queue[0];
    }

    /**
     * Returns an array containing all of the elements in this queue.
     * The elements are in no particular order.
     *
     * <p>The returned array will be "safe" in that no references to it are
     * maintained by this queue.  (In other words, this method must allocate
     * a new array).  The caller is thus free to modify the returned array.
     *
     * <p>This method acts as bridge between array-based and collection-based
     * APIs.
     *
     * @return an array containing all of the elements in this queue
     */
    public Object[] toArray() {
        return Arrays.copyOf(queue, size);
    }

    /**
     * Returns an array containing all of the elements in this queue; the
     * runtime type of the returned array is that of the specified array.
     * The returned array elements are in no particular order.
     * If the queue fits in the specified array, it is returned therein.
     * Otherwise, a new array is allocated with the runtime type of the
     * specified array and the size of this queue.
     *
     * <p>If the queue fits in the specified array with room to spare
     * (i.e., the array has more elements than the queue), the element in
     * the array immediately following the end of the collection is set to
     * {@code null}.
     *
     * <p>Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs.  Further, this method allows
     * precise control over the runtime type of the output array, and may,
     * under certain circumstances, be used to save allocation costs.
     *
     * <p>Suppose <tt>x</tt> is a queue known to contain only strings.
     * The following code can be used to dump the queue into a newly
     * allocated array of <tt>String</tt>:
     *
     * <pre>
     *     String[] y = x.toArray(new String[0]);</pre>
     *
     * Note that <tt>toArray(new Object[0])</tt> is identical in function to
     * <tt>toArray()</tt>.
     *
     * @param a the array into which the elements of the queue are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose.
     * @return an array containing all of the elements in this queue
     * @throws ArrayStoreException if the runtime type of the specified array
     *         is not a supertype of the runtime type of every element in
     *         this queue
     * @throws NullPointerException if the specified array is null
     */
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        if (a.length < size)
            // Make a new array of a's runtime type, but my contents:
            return (T[]) Arrays.copyOf(queue, size, a.getClass());
        System.arraycopy(queue, 0, a, 0, size);
        if (a.length > size)
            a[size] = null;
        return a;
    }

    public int size() {
        return size;
    }

    /**
     * Removes all of the elements from this priority queue.
     * The queue will be empty after this call returns.
     */
    public void clear() {
        for (int i = 0; i < size; i++)
            queue[i] = null;
        size = 0;
    }

    @SuppressWarnings("unchecked")
    public E poll() {
        if (size == 0)
            return null;
        int s = --size;
        E result = (E) queue[0];
        E x = (E) queue[s];
        queue[s] = null;
        if (s != 0)
            siftDown(0, x);
        return result;
    }

    /**
     * Inserts item x at position k, maintaining heap invariant by
     * demoting x down the tree repeatedly until it is less than or
     * equal to its children or is a leaf.
     *
     * @param k the position to fill
     * @param x the item to insert
     */
    private void siftDown(int k, E x) {
        if (comparator != null)
            siftDownUsingComparator(k, x);
        else
            siftDownComparable(k, x);
    }

    @SuppressWarnings("unchecked")
    private void siftDownComparable(int k, E x) {
        Comparable<? super E> key = (Comparable<? super E>)x;
        int half = size >>> 1;        // loop while a non-leaf
        while (k < half) {
            int child = (k << 1) + 1; // assume left child is least
            Object c = queue[child];
            int right = child + 1;

            if (right < size) {
                if (rememberdOrder[k] == ORDER_UNKNOWN) {
                    if (((Comparable<? super E>) c).compareTo((E) queue[right]) > 0) {
                        rememberdOrder[k] = ORDER_RIGHT_MIN;
                        child = right;
                        c = queue[right];
                    } else {
                        rememberdOrder[k] = ORDER_LEFT_MIN;
                    }
                } else if (rememberdOrder[k] == ORDER_RIGHT_MIN) {
                    child = right;
                    c = queue[right];
                }
            }

            if (key.compareTo((E) c) <= 0)
                break;

            rememberdOrder[k] = ORDER_UNKNOWN;
            queue[k] = c;
            k = child;
        }
        queue[k] = key;
    }

    @SuppressWarnings("unchecked")
    private void siftDownUsingComparator(int k, E x) {
        int half = size >>> 1;
        while (k < half) {
            int child = (k << 1) + 1;
            Object c = queue[child];
            int right = child + 1;
            if (right < size) {
                if (rememberdOrder[k] == ORDER_UNKNOWN) {
                    if (comparator.compare((E) c, (E) queue[right]) > 0) {
                        rememberdOrder[k] = ORDER_RIGHT_MIN;
                        child = right;
                        c = queue[right];
                    } else {
                        rememberdOrder[k] = ORDER_LEFT_MIN;
                    }
                } else if (rememberdOrder[k] == ORDER_RIGHT_MIN) {
                    child = right;
                    c = queue[right];
                }
            }

            if (comparator.compare(x, (E) c) <= 0)
                break;

            rememberdOrder[k] = ORDER_UNKNOWN;
            queue[k] = c;
            k = child;
        }
        queue[k] = x;
    }

    /**
     * Establishes the heap invariant (described above) in the entire tree,
     * assuming nothing about the order of the elements prior to the call.
     */
    @SuppressWarnings("unchecked")
    public void heapify() {
        for (int i = (size >>> 1) - 1; i >= 0; i--)
            siftDown(i, (E) queue[i]);
    }

    /**
     * Returns the comparator used to order the elements in this
     * queue, or {@code null} if this queue is sorted according to
     * the {@linkplain Comparable natural ordering} of its elements.
     *
     * @return the comparator used to order this queue, or
     *         {@code null} if this queue is sorted according to the
     *         natural ordering of its elements
     */
    public Comparator<? super E> comparator() {
        return comparator;
    }

}

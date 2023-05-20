package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
                getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         * <p>
         * If there are no more records in the left source, this method should
         * do nothing.
         * <p>
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            leftBlockIterator = QueryOperator.getBlockIterator(
                    leftSourceIterator,
                    getLeftSource().getSchema(),
                    numBuffers - 2
            );
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         * <p>
         * If there are no more records in the right source, this method should
         * do nothing.
         * <p>
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            rightPageIterator = QueryOperator.getBlockIterator(
                    rightSourceIterator,
                    getRightSource().getSchema(),
                    1
            );
            rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         * <p>
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */

        public int count = 0;
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            while (true) {
                count += 1;
                // `Right` / `Top` descriptions is based on gif images highlighted square position.
                // https://cs186.gitbook.io/project/assignments/proj3/part-1-join-algorithms#task-1-nested-loop-joins
                boolean isInRight = !rightPageIterator.hasNext();
                boolean isInRightMost = isInRight && !rightSourceIterator.hasNext();
                boolean isInTop = !leftBlockIterator.hasNext();
                boolean isInTopMost = isInTop && !leftSourceIterator.hasNext();
                boolean isInTopRight = isInRight && isInTop;

                if (isInRightMost && isInTopMost) {
                    return null;
                }

                if (leftRecord == null && !isInTop) {
                    leftRecord = leftBlockIterator.next();
                }

                if (!isInRight) {
                    Record rightRecord = rightPageIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                    continue;
                }

                if (isInRightMost && isInTopRight) {
                    leftRecord = null;
                    fetchNextLeftBlock();
                    rightSourceIterator.reset();
                    fetchNextRightPage();
                    continue;
                }

                if (!isInRightMost && isInTopRight) {
                    leftRecord = null;
                    leftBlockIterator.reset();
                    fetchNextRightPage();
                    continue;
                }

                // In right but not right most and top.
                leftRecord = null;
                rightPageIterator.reset();
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}

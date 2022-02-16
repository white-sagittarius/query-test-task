package org.query.calc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;

import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;

import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.doubles.Double2DoubleAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2DoubleMap;
import it.unimi.dsi.fastutil.doubles.Double2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2ObjectMap;
import it.unimi.dsi.fastutil.doubles.DoubleComparators;
import it.unimi.dsi.fastutil.doubles.DoubleDoubleImmutablePair;
import it.unimi.dsi.fastutil.doubles.DoubleDoubleMutablePair;
import it.unimi.dsi.fastutil.doubles.DoubleDoublePair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

public class QueryCalcImpl implements QueryCalc {

  private static final int RESULT_LIMIT = 10;
  private static final char COLUMN_SEPARATOR = ' ';

  @Override
  public void select(Path t1, Path t2, Path t3X, Path output) throws IOException {
    // - t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each
    //  line contains exactly one row, that contains two numbers parsable by Double.parse(): value for column a and
    //  x respectively.See test resources for examples.
    // - t2 is a file contains table "t2" with columns "b" and "y". Same format.
    // - t3 is a file contains table "t3" with columns "c" and "z". Same format.
    // - output is table stored in the same format: first line is a number of rows, then each line is one row that
    //  contains two numbers: value for column a and s.
    //
    // Number of rows of all three tables lays in range [0, 1_000_000].
    // It's guaranteed that full content of all three tables fits into RAM.
    // It's guaranteed that full outer join of at least one pair (t1xt2 or t2xt3 or t1xt3) of tables can fit into RAM.
    //
    // TODO: Implement following query, put a reasonable effort into making it efficient from perspective of
    //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
    //  from a maven central.
    //
    // SELECT a, SUM(x * y * z) AS s FROM
    // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
    // ON a < b + c
    // GROUP BY a
    // STABLE ORDER BY s DESC
    // LIMIT 10;
    //
    // Note: STABLE is not a standard SQL command. It means that you should preserve the original order.
    // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
    // In case multiple occurrences, you may assume that group has a row number of the first occurrence.

    // ** IMPLEMENTATION NOTICE **
    // - proper error handling is left out deliberately not to distract from the algorithmic part
    // - component breakdown (single responsibility principle) is left out deliberately to keep everything compact and simple
    // - concrete types are used instead of interfaces to emphasize data structures involved
    //
    // Main complexity lies in 3 table join on "a < b + c" condition.
    // SUM(x * y * z) can be replaced with (x1..xN)*(y1..yM)*(z1..zK) with x1..xN mapped to some value "a",
    // and y1..yM, z1..K mapped to some "b" and "c", so that condition "a < b + c" is true
    //
    // a < b + c => a - b < c => SUM(z1..zK) can be computed eagerly for every "c" in linear time. t3 is sorted by "c" descending
    // in log-linear time (AVL tree)
    //
    // SUM(x1..xN) for given "a" is computed as a part of grouping process (linked hash map)
    //
    // t2 table is scanned line-by-line
    //
    // min-heap is used to select top 10 results (intermediate stack was used to output values in correct DESC order)
    //
    // Alternative approach might use Red-Black tree instead of AVL for t3 (Double2DoubleRBTreeMap) and simple array-based priority-queue
    // instead of min-heap because we only ever need top 10 results (ObjectArrayPriorityQueue). Decision should be based on the real
    // data testing results

    @Cleanup
    TableIterator t1Iterator = new TableIterator(t1);

    @Cleanup
    TableIterator t2Iterator = new TableIterator(t2);

    @Cleanup
    TableIterator t3Iterator = new TableIterator(t3X);

    // make sure size(t3) >= size(t2)
    if (t3Iterator.getNumberOfRows() < t2Iterator.getNumberOfRows()) {
      TableIterator temp = t3Iterator;

      t3Iterator = t2Iterator;
      t2Iterator = temp;
    }

    // AVL tree mapping "c" to "z", grouped by "c" and ordered DESC by "c"
    Double2DoubleAVLTreeMap t3 = new Double2DoubleAVLTreeMap(DoubleComparators.OPPOSITE_COMPARATOR);

    while (t3Iterator.hasNext()) {
      DoubleDoublePair cAndZ = t3Iterator.next();

      double c = cAndZ.leftDouble();
      double z = cAndZ.rightDouble();

      t3.addTo(round(c), z);
    }

    double sumOfZ = 0.0;

    // if a1 < b1 + c1 => a1 - b1 < c1 is true for given a1 and b1, then it will be true for any c > c1
    // we use this property to precompute sum of z values for all c > c[i], i = 0..n
    for (Double2DoubleMap.Entry t3Row : t3.double2DoubleEntrySet()) {
      sumOfZ += t3Row.getDoubleValue();

      t3Row.setValue(sumOfZ);
    }

    // group results by "a" column (key) with the value being a combination of "x" and SUM
    Double2ObjectLinkedOpenHashMap<DoubleDoubleMutablePair> accumulator = new Double2ObjectLinkedOpenHashMap<>(
      t1Iterator.getNumberOfRows());

    while (t1Iterator.hasNext()) {
      DoubleDoublePair aAndX = t1Iterator.next();

      double a = aAndX.leftDouble();
      double x = aAndX.rightDouble();

      accumulator.merge(round(a), DoubleDoubleMutablePair.of(x, 0.0),
                        (oldValue, newValue) -> oldValue.left(oldValue.leftDouble() + newValue.leftDouble()));
    }

    while (t2Iterator.hasNext()) {
      DoubleDoublePair bAndY = t2Iterator.next();

      double b = bAndY.leftDouble();
      double y = bAndY.rightDouble();

      if (round(y) == 0.0) {
        continue;
      }

      for (Double2ObjectMap.Entry<DoubleDoubleMutablePair> entry : accumulator.double2ObjectEntrySet()) {
        double a = entry.getDoubleKey();

        DoubleDoubleMutablePair xAndSum = entry.getValue();

        double x = xAndSum.leftDouble();

        if (round(x) == 0.0) {
          continue;
        }

        // a < b + c => a - b < c => head map returns all such "c", we need the smallest one
        Double2DoubleMap.Entry cAndSumOfZ = t3.headMap(round(a - b)).double2DoubleEntrySet().last();

        if (cAndSumOfZ == null) {
          continue;
        }

        xAndSum.right(xAndSum.rightDouble() + xAndSum.leftDouble() * y * cAndSumOfZ.getDoubleValue());
      }
    }

    // min-heap is used to select top N results
    ObjectHeapPriorityQueue<DoubleDoublePair> result = new ObjectHeapPriorityQueue<>(RESULT_LIMIT, Comparator.comparingDouble(
      DoubleDoublePair::rightDouble));

    for (Double2ObjectMap.Entry<DoubleDoubleMutablePair> entry : accumulator.double2ObjectEntrySet()) {
      double a = entry.getDoubleKey();
      double sum = round(entry.getValue().rightDouble());

      if (result.size() == RESULT_LIMIT) {
        double smallestSumInResult = result.first().rightDouble();

        if (sum <= smallestSumInResult) {
          continue;
        }

        result.dequeue();
      }

      result.enqueue(DoubleDoubleImmutablePair.of(a, sum));
    }

    writeResult(result, output);
  }

  private static void writeResult(PriorityQueue<DoubleDoublePair> result, Path output) throws IOException {
    ObjectArrayList<DoubleDoublePair> stack = new ObjectArrayList<>(result.size());

    // intermediate stack is used to turn min-heap ASC sorting into DESC sorting needed for output
    while (!result.isEmpty()) {
      stack.push(result.dequeue());
    }

    @Cleanup
    BufferedWriter resultWriter = Files.newBufferedWriter(output);

    resultWriter.write(Integer.toString(stack.size()));

    while (!stack.isEmpty()) {
      DoubleDoublePair aAndSum = stack.pop();

      resultWriter.newLine();
      resultWriter.write(Double.toString(aAndSum.leftDouble()));
      resultWriter.write(COLUMN_SEPARATOR);
      resultWriter.write(Double.toString(aAndSum.rightDouble()));
    }
  }

  private static double round(double value) {
    return Math.round(value * 1e8) * 1e-8;
  }

  private static class TableIterator implements Iterator<DoubleDoublePair>, AutoCloseable {

    private final DoubleDoubleMutablePair pair = DoubleDoubleMutablePair.of(0.0, 0.0);
    private final BufferedReader reader;

    @Getter
    private final int numberOfRows;

    private int currentRow;

    public TableIterator(Path path) throws IOException {
      this.reader = Files.newBufferedReader(path);
      this.numberOfRows = Integer.parseInt(reader.readLine());
    }

    @Override
    public boolean hasNext() {
      return currentRow < numberOfRows;
    }

    @Override
    @SneakyThrows
    public DoubleDoublePair next() {
      String line = reader.readLine();

      currentRow++;

      int indexOfSeparator = line.indexOf(COLUMN_SEPARATOR);

      double firstColumn = Double.parseDouble(line.substring(0, indexOfSeparator));
      double secondColumn = Double.parseDouble(line.substring(indexOfSeparator + 1));

      return pair.left(firstColumn).right(secondColumn);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

  }

}

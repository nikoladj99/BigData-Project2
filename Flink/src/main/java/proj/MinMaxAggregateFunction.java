package proj;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MinMaxAggregateFunction implements AggregateFunction<CheckIn, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> {
    @Override
    public Tuple3<String, Integer, Integer> createAccumulator() {
        return new Tuple3<>("", Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    @Override
    public Tuple3<String, Integer, Integer> add(CheckIn value, Tuple3<String, Integer, Integer> acc) {
        acc.f0 = value.getLocation_id();
        acc.f1 = acc.f1 > value.getTime_spent() ? acc.f1 : value.getTime_spent();
        acc.f2 = acc.f2 < value.getTime_spent() ? acc.f2 : value.getTime_spent();

        return new Tuple3<>(acc.f0, acc.f1, acc.f2);
    }

    @Override
    public Tuple3<String, Integer, Integer> getResult(Tuple3<String, Integer, Integer> acc) {
        return new Tuple3<>(acc.f0, acc.f1, acc.f2);
    }

    @Override
    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> acc1, Tuple3<String, Integer, Integer> acc2) {
        return new Tuple3<>(acc1.f0, acc1.f1 > acc2.f1 ? acc1.f1 : acc2.f1, acc1.f2 < acc2.f2 ? acc1.f2 : acc2.f2);
    }
}

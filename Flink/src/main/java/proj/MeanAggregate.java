package proj;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

class MeanAggregate implements AggregateFunction<CheckIn, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple3<String, Double, Integer> createAccumulator() {
        return new Tuple3<>("", 0D, 0);
    }

    @Override
    public Tuple3<String, Double, Integer> add(CheckIn value, Tuple3<String, Double, Integer> acc) {
        acc.f0 = value.getLocation_id();
        acc.f1 += value.getTime_spent();
        return new Tuple3<>(acc.f0, acc.f1, acc.f2 + 1);
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> acc) {
        return new Tuple2<>(acc.f0, acc.f1 / acc.f2);
    }

    @Override
    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> acc1, Tuple3<String, Double, Integer> acc2) {
        return new Tuple3<>(acc1.f0, acc1.f1 + acc2.f1, acc1.f2 + acc2.f2);
    }
}
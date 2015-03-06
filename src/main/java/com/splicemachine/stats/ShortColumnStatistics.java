package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.cardinality.ShortCardinalityEstimator;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.UniformShortDistribution;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.ShortFrequentElements;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public class ShortColumnStatistics extends BaseColumnStatistics<Short> {

    private ShortCardinalityEstimator cardinalityEstimator;
    private ShortFrequentElements frequentElements;
    private short min;
    private short max;
    private Distribution<Short> distribution;


    public ShortColumnStatistics(int columnId,
                                 ShortCardinalityEstimator cardinalityEstimator,
                                  ShortFrequentElements frequentElements,
                                  short min,
                                  short max,
                                  long totalBytes,
                                  long totalCount,
                                  long nullCount,
                                 long minCount) {
        super(columnId, totalBytes, totalCount, nullCount,minCount);
        this.cardinalityEstimator = cardinalityEstimator;
        this.frequentElements = frequentElements;
        this.min = min;
        this.max = max;
        this.distribution = new UniformShortDistribution(this);
    }

    @Override public long cardinality() { return cardinalityEstimator.getEstimate(); }
    @Override public FrequentElements<Short> topK() { return frequentElements; }
    @Override public Short minValue() { return min; }
    @Override public Short maxValue() { return max; }
    public short min(){ return min; }
    public short max(){ return max; }

    @Override
    public ColumnStatistics<Short> getClone() {
        return new ShortColumnStatistics(columnId,cardinalityEstimator.newCopy(),
                frequentElements.newCopy(),
                min,
                max,
                totalBytes,
                totalCount,
                nullCount,
                minCount);
    }

    @Override
    public ColumnStatistics<Short> merge(ColumnStatistics<Short> other) {
        assert other instanceof ShortColumnStatistics: "Cannot merge statistics of type "+ other.getClass();
        ShortColumnStatistics o = (ShortColumnStatistics)other;
        cardinalityEstimator = cardinalityEstimator.merge(o.cardinalityEstimator);
        frequentElements = frequentElements.merge(o.frequentElements);
        if(o.min<min)
            min = o.min;
        if(o.max>max)
            max = o.max;
        totalBytes+=o.totalBytes;
        totalCount+=o.totalCount;
        nullCount+=o.nullCount;
        return this;
    }

    @Override
    public Distribution<Short> getDistribution() {
        return distribution;
    }

    public static Encoder<ShortColumnStatistics> encoder(){
        return EncDec.INSTANCE;
    }

    static class EncDec implements Encoder<ShortColumnStatistics> {
        public static final EncDec INSTANCE = new EncDec();

        @Override
        public void encode(ShortColumnStatistics item,DataOutput encoder) throws IOException {
            BaseColumnStatistics.write(item, encoder);
            encoder.writeShort(item.min);
            encoder.writeShort(item.max);
            CardinalityEstimators.shortEncoder().encode(item.cardinalityEstimator, encoder);
            FrequencyCounters.shortEncoder().encode(item.frequentElements,encoder);
        }

        @Override
        public ShortColumnStatistics decode(DataInput decoder) throws IOException {
            int colId = decoder.readInt();
            long totalBytes = decoder.readLong();
            long totalCount = decoder.readLong();
            long nullCount = decoder.readLong();
            long minCount = decoder.readLong();
            short min = decoder.readShort();
            short max = decoder.readShort();
            ShortCardinalityEstimator cardinalityEstimator = CardinalityEstimators.shortEncoder().decode(decoder);
            ShortFrequentElements frequentElements = FrequencyCounters.shortEncoder().decode(decoder);
            return new ShortColumnStatistics(colId,cardinalityEstimator,frequentElements,min,max,totalBytes,totalCount,nullCount,minCount);
        }
    }
}

package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:23
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseInputFormat
 * @Description:
 **/
public abstract class BaseRichInputFormat<OT, T extends InputSplit> extends RichInputFormat<OT, T> {

    /**
     * Configures this input format. Since input formats are instantiated generically and hence parameterless,
     * this method is the place where the input formats set their basic fields based on configuration values.
     * <p>
     * This method is always called first on a newly instantiated input format.
     *
     * @param parameters The configuration with all parameters (note: not the Flink config but the TaskConfig).
     */
    @Override
    public void configure(Configuration parameters) {

    }

    /**
     * Gets the basic statistics from the input described by this format. If the input format does not know how
     * to create those statistics, it may return null.
     * This method optionally gets a cached version of the statistics. The input format may examine them and decide
     * whether it directly returns them without spending effort to re-gather the statistics.
     * <p>
     * When this method is called, the input format it guaranteed to be configured.
     *
     * @param cachedStatistics The statistics that were cached. May be null.
     * @return The base statistics for the input, or null, if not available.
     */
    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    /**
     * Creates the different splits of the input that can be processed in parallel.
     * <p>
     * When this method is called, the input format it guaranteed to be configured.
     *
     * @param minNumSplits The minimum desired number of splits. If fewer are created, some parallel
     *                     instances may remain idle.
     * @return The splits of this input that can be processed in parallel.
     * @throws IOException Thrown, when the creation of the splits was erroneous.
     */
    @Override
    public T[] createInputSplits(int minNumSplits) throws IOException {
        return null;
    }

    /**
     * Gets the type of the input splits that are processed by this input format.
     *
     * @param inputSplits
     * @return The type of the input splits.
     */
    @Override
    public InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
        return null;
    }

    /**
     * Opens a parallel instance of the input format to work on a split.
     * <p>
     * When this method is called, the input format it guaranteed to be configured.
     *
     * @param split The split to be opened.
     * @throws IOException Thrown, if the spit could not be opened due to an I/O problem.
     */
    @Override
    public void open(T split) throws IOException {

    }

    /**
     * Method used to check if the end of the input is reached.
     * <p>
     * When this method is called, the input format it guaranteed to be opened.
     *
     * @return True if the end is reached, otherwise false.
     * @throws IOException Thrown, if an I/O error occurred.
     */
    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    /**
     * Reads the next record from the input.
     * <p>
     * When this method is called, the input format it guaranteed to be opened.
     *
     * @param reuse Object that may be reused.
     * @return Read record.
     * @throws IOException Thrown, if an I/O error occurred.
     */
    @Override
    public OT nextRecord(OT reuse) throws IOException {
        return null;
    }

    /**
     * Method that marks the end of the life-cycle of an input split. Should be used to close channels and streams
     * and release resources. After this method returns without an error, the input is assumed to be correctly read.
     * <p>
     * When this method is called, the input format it guaranteed to be opened.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {

    }
}

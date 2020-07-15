package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:06
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: ArgusInputFormatSource
 * @Description:
 **/
public class ArgusInputFormatSource<OUT> extends RichParallelSourceFunction<OUT> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private TypeInformation<OUT> typeInfo;
    private transient TypeSerializer<OUT> serializer;

    private InputFormat<OUT, InputSplit> format;

    private transient InputSplitProvider provider;
    private transient Iterator<InputSplit> splitIterator;

    private volatile boolean isRunning = true;

    @SuppressWarnings("unchecked")
    public ArgusInputFormatSource(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
        this.format = (InputFormat<OUT, InputSplit>) format;
        this.typeInfo = typeInfo;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(Configuration parameters) throws Exception {
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        if (format instanceof RichInputFormat) {
            ((RichInputFormat) format).setRuntimeContext(context);
        }
        format.configure(parameters);

        provider = context.getInputSplitProvider();
        serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
        splitIterator = getInputSplits();
        isRunning = splitIterator.hasNext();
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        try {

            Counter completedSplitsCounter = getRuntimeContext().getMetricGroup().counter("numSplitsProcessed");
            if (isRunning && format instanceof RichInputFormat) {
                ((RichInputFormat) format).openInputFormat();
            }

            OUT nextElement = serializer.createInstance();
            while (isRunning) {
                format.open(splitIterator.next());

                // for each element we also check if cancel
                // was called by checking the isRunning flag

                while (isRunning && !format.reachedEnd()) {
                    nextElement = format.nextRecord(nextElement);
                    if (nextElement != null) {
                        ctx.collect(nextElement);
                    } else {
                        break;
                    }
                }
                format.close();
                completedSplitsCounter.inc();

                if (isRunning) {
                    isRunning = splitIterator.hasNext();
                }
            }
        } finally {
            format.close();
            if (format instanceof RichInputFormat) {
                ((RichInputFormat) format).closeInputFormat();
            }
            isRunning = false;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        format.close();
        if (format instanceof RichInputFormat) {
            ((RichInputFormat) format).closeInputFormat();
        }
    }

    /**
     * Returns the {@code InputFormat}. This is only needed because we need to set the input
     * split assigner on the {@code StreamGraph}.
     */
    public InputFormat<OUT, InputSplit> getFormat() {
        return format;
    }

    private Iterator<InputSplit> getInputSplits() {

        return new Iterator<InputSplit>() {

            private InputSplit nextSplit;

            private boolean exhausted;

            @Override
            public boolean hasNext() {
                if (exhausted) {
                    return false;
                }

                if (nextSplit != null) {
                    return true;
                }

                final InputSplit split;
                try {
                    split = provider.getNextInputSplit(getRuntimeContext().getUserCodeClassLoader());
                } catch (InputSplitProviderException e) {
                    throw new RuntimeException("Could not retrieve next input split.", e);
                }

                if (split != null) {
                    this.nextSplit = split;
                    return true;
                } else {
                    exhausted = true;
                    return false;
                }
            }

            @Override
            public InputSplit next() {
                if (this.nextSplit == null && !hasNext()) {
                    throw new NoSuchElementException();
                }

                final InputSplit tmp = this.nextSplit;
                this.nextSplit = null;
                return tmp;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}

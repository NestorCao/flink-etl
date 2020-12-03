package com.dtstack.flinkx.step;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public abstract class BaseStep {

	public abstract DataStream<Row> run(DataStream<Row> dataStream);

}

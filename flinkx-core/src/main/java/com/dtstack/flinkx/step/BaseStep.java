package com.dtstack.flinkx.step;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;

public abstract class BaseStep {

	public abstract DataStream<JSONObject> run(DataStream<JSONObject> dataStream);



}

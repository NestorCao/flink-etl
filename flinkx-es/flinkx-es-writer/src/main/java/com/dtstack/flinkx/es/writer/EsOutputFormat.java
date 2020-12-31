/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.es.writer;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.es.EsUtil;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The OutputFormat class of ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */

public class EsOutputFormat extends BaseRichOutputFormat {

    protected String address;

    protected String username;

    protected String password;

    protected List<Integer> idColumnIndices;

    protected List<String> idColumnValues;

    protected List<String> idColumnTypes;

    protected String index;

    protected String type;

    protected List<String> columnTypes;

    protected List<String> columnNames;

    protected Map<String,Object> clientConfig;
    protected Integer bulkAction;
    protected Integer timeOut;
    private transient RestHighLevelClient client;
    
    private BulkProcessor bulkProcessor;

    @Override
    public void configure(Configuration configuration) {
        client = EsUtil.getClient(address, username, password, clientConfig);
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions(); 
                LOG.debug("Executing bulk [{}] with {} requests",
                        executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    BulkResponse response) {
                if (response.hasFailures()) { 
                    LOG.warn("Bulk [{}] executed with failures", executionId); 
                    //待补充，需要将入库失败的记录打印至日志中
                    
                } else {
                	LOG.debug("Bulk [{}] completed in {} milliseconds",
                            executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                    Throwable failure) {
            	LOG.error("Failed to execute bulk", failure); 
            }
        };

        BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
        builder.setBulkActions(bulkAction);
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB)); 
        builder.setConcurrentRequests(1); 
        builder.setFlushInterval(TimeValue.timeValueSeconds(timeOut)); 
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3)); 
        bulkProcessor = builder.build();
        }

    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(JSONObject row) throws WriteRecordException {
    	   IndexRequest request =  new IndexRequest(index, type);
           request = request.source(row.toJSONString(),XContentType.JSON);
           bulkProcessor.add(request);
       
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        notSupportBatchWrite("ElasticsearchWriter");
    }

    @Override
    public void closeInternal() throws IOException {
        if(client != null) {
            client.close();
        }
    }


    

}

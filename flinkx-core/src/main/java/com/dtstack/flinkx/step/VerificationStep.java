package com.dtstack.flinkx.step;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.flinkx.config.StepConfig;
import com.dtstack.flinkx.util.MapUtil;

import lombok.Data; 

public class VerificationStep extends BaseStep{
	private List<String> includeColumn;
	private List<Verification> vers;
	
	 public VerificationStep(StepConfig stepConfig) {
		includeColumn = (List<String>) stepConfig.getParameter().getVal("includeColumn");	
		vers =  (List<Verification>) stepConfig.getParameter().getVal("verification");	
		     
	}   
	 @Override
		public DataStream<Row> run(DataStream<Row> dataStream) {
		    
			return dataStream.filter(new FilterFunction<Row>() {

				@Override
				public boolean filter(Row row) throws Exception {
					Map<String,Object> data = null;
					return verifyData( data, vers, includeColumn);
				}
			});
		}
	 
	private Logger alldataLogger = LoggerFactory.getLogger("dataLogger");
	    private Logger verifyLogger = LoggerFactory.getLogger("verifyLogger");
 

	    private boolean verifyData( Map<String, Object> data, List<Verification> vers, List<String> includeColumn) {
	    	boolean columnIntegrity = verifyColumnIntegrity(data, includeColumn);
	    	if(columnIntegrity == false)
	    		return false;
	    	boolean columnCorrect = verifyColumnCorrect( data,vers);
            if(columnCorrect == false)
            	return false;
            return true;
	    }

	    private boolean verifyColumnIntegrity(Map<String, Object> resourceData, List<String> includeColumn) {
	        boolean columnIntegrity = true;
	        for (String ic : includeColumn) {
	            if(!resourceData.containsKey(ic)){
	                verifyLogger.error("【未校验通过:字段缺失】{}>>>>>{}",  ic);
	                columnIntegrity =false;
	                break;
	            }
	        }
	        return columnIntegrity;
	    }

	    private boolean verifyColumnCorrect(Map<String, Object> resourceData, List<Verification> vers) {

	        boolean columnCorrect = true;
	        for (Verification ver : vers) {
	            String field = ver.getField();
	            Object val = resourceData.get(field);
	            boolean verifyCondition =  resourceData.containsKey(field);
	            if(verifyCondition && ver.isRequire() && objIsNull(val)){
	                verifyLogger.error("【未校验通过:字段{}要求必填】{}>>>>>{}",field);
	                columnCorrect = false;
	                break;
	            }else if (verifyCondition && val != null && ver.getLength() != 0 && val.toString().length() != ver.getLength()){
	                verifyLogger.error("【未校验通过:字段{}要求长度为{}，实际长度为{}】{}>>>>>{}", field, ver.getLength(), val.toString().length());
	                columnCorrect = false;
	                break;
	            }else if (verifyCondition && val != null && !objIsNull(ver.getType()) && !VerifyType.valueOf(ver.getType().toUpperCase()).verify(val, ver.getDateFormat())){
	                verifyLogger.error("【未校验通过，字段{}类型要求为{}】{}>>>>>{}", field, ver.getType() + (ver.getType().toUpperCase().equals("DATATIME") ? "---" + ver.getDateFormat() : ""));
	                columnCorrect = false;
	                break;
	            }
	        }
	        return columnCorrect;

	    }

	  

	    private boolean objIsNull(Object val) {
	        return null == val || "".equals(val.toString().trim());

	    }


		
	    }
@Data
class Verification {
    private String field;
    private boolean require;
    private int length;
    private String type;
    private String dateFormat;
}


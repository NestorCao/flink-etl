package com.dtstack.flinkx.step;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flinkx.config.StepConfig;
import com.dtstack.flinkx.exception.FieldLengthErrorException;
import com.dtstack.flinkx.exception.FieldNotExistException;
import com.dtstack.flinkx.exception.FieldNotMatchesException;
import com.esotericsoftware.minlog.Log; 

public class ConvertionStep extends BaseStep{
	 private JSONObject convertionRule;
	 public ConvertionStep(StepConfig stepConfig) {
		 convertionRule = new JSONObject((HashMap)stepConfig.getParameter().getVal("rule"));	
		     
	}   
	 @Override
		public DataStream<JSONObject> run(DataStream<JSONObject> dataStream) {
		    JSONObject localConvertionRule = convertionRule;;
			return dataStream.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
				  	@Override
					public void flatMap(JSONObject origin, Collector<JSONObject> out) throws Exception {
							        convertion(origin, localConvertionRule);
							        JSONObject result = (JSONObject) origin.clone();
							        moveField(origin,localConvertionRule,result,result);  
					                out.collect(result);             
				  	}
					 
					 private void convertion(JSONObject origin, JSONObject convertionRule) throws Exception {
				             Set<String> keySet = convertionRule.keySet();
							 for(String field : keySet) {
								JSONObject fieldRule = convertionRule.getJSONObject(field);
								Object fieldVal = origin.get(field);
								
								//字段是否为空检验
								Boolean required = fieldRule.getBoolean("required");
								if(required !=null && required == true ) {
									if(fieldVal == null || (fieldVal instanceof String && fieldVal.equals(""))){
										  throw new FieldNotExistException(
								                    "字段不存在异常: field " + field + " is required", field);
									}
								}
								//字段长度检验
								Integer length = fieldRule.getInteger("maxLength");
								if(length !=null && fieldVal != null) {
										String str = String.valueOf(fieldVal);
									    if(str.length() > length) {
											  throw new FieldLengthErrorException( "字段长度超出限制: field " + field +"的最大长度为"+length , field);
										    	
									    }
										
									
								}
								//日期格式校验
								String dateFormat = fieldRule.getString("dateFormat");
								if(dateFormat !=null && fieldVal != null) {
								        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
						                try {
								          sdf.parse(String.valueOf(fieldVal));
						                }catch(Exception e) {
						                	  throw new FieldNotMatchesException("日期字段: field " + field +"的格式不符合要求 ", field);
										
						                }
										
									}
								
								  //日期格式转化
								String dateFormatTo = fieldRule.getString("dateFormatTo");
								if(dateFormat !=null &&dateFormatTo !=null && fieldVal != null ) {
								        try {
								        	SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
								              
								        	Date srcDate = sdf.parse(String.valueOf(fieldVal));
								            sdf = new SimpleDateFormat(dateFormatTo);
								            origin.put(field, sdf.format(srcDate));
						                }catch(Exception e) {
						                	 Log.error(e.getMessage());
						                }
										
									}
								
								//值转化
								Map valueMapper = fieldRule.getObject("valueMapper",HashMap.class);
								if(valueMapper !=null && fieldVal != null ) {
								        	Object newVal = valueMapper.get(fieldVal);
								        	if(newVal!=null) {
								        		origin.put(field, newVal);
								        	}
						            	
									}
								
								//字符串替换
								Map strReplaceMapper = fieldRule.getObject("strReplace",HashMap.class);
								if(strReplaceMapper !=null && fieldVal != null ) {
								        	Set<Entry<String, String>> set = strReplaceMapper.entrySet();
							        		for(Entry<String, String> entry : set) {
							        			if(fieldVal.toString().contains(entry.getKey())){
							        				String newVal = fieldVal.toString().replace(entry.getKey(), entry.getValue());
							        				origin.put(field, newVal); 
							        				break;
							        			}
							        		}
						               
										
								}
								//字段名转化             
								String rename = fieldRule.getString("rename");
								if(rename !=null && fieldVal != null) {
									origin.put(rename, fieldVal);
									origin.remove(field);
								} 
								//字段拷贝             
								String copyTo = fieldRule.getString("copyTo");
								if(copyTo !=null && fieldVal != null) {
									origin.put(copyTo, fieldVal);
								} 
								
							   //遍历下一级JSON对象
								JSONObject properties = fieldRule.getJSONObject("properties");
								 if(properties!=null) {
							    	 convertion(origin.getJSONObject(field),properties);
							     }
							 }
				}
						private void moveField(JSONObject origin, JSONObject convertionRule, JSONObject startNode,JSONObject currentNode) {
							     Set<String> keySet = convertionRule.keySet();
								 for(String field : keySet) {
									 
									JSONObject fieldRule = convertionRule.getJSONObject(field);
								    String moveTo = fieldRule.getString("moveTo");    
								    //移动该field
								    if(moveTo!=null) {
								    	String[] paths = moveTo.split(",");
								    	int pathLength = paths.length;
								    	JSONObject cursor = startNode;
								    	for(int i = 0 ; i < pathLength-1;i++) {
								    		if(cursor.get(paths[i]) == null) {
								    			cursor.put(paths[i], new JSONObject());
								    			
								    		}
								    		cursor = cursor.getJSONObject(paths[i]);
								    	}
								    	cursor.put(paths[pathLength-1], origin.get(field));
								    	 currentNode.remove(field);
										  
								    }
								    
								    //遍历下一级JSON对象
									JSONObject properties = fieldRule.getJSONObject("properties");
									 if(properties!=null) {
										 moveField(origin.getJSONObject(field),properties,startNode,currentNode.getJSONObject(field));
								     }
									 //当field下面没有任何的子元素，清除该field
                                     if(currentNode.get(field) != null && currentNode.get(field) instanceof JSONObject && currentNode.getJSONObject(field).isEmpty()){
										  currentNode.remove(field);         
									 }
									
								 }
								
						}
			});
				            
			}
	

}
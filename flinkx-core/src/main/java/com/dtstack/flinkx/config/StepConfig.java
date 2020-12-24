package com.dtstack.flinkx.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import com.dtstack.flinkx.config.WriterConfig.ParameterConfig;
import com.dtstack.flinkx.config.WriterConfig.ParameterConfig.ConnectionConfig;

public class StepConfig extends AbstractConfig{
	
	public static String KEY_PARAMETER_CONFIG = "parameter";
	public static String KEY_WRITER_NAME = "name";
	 ParameterConfig parameter;

	    
	public StepConfig(Map<String, Object> map) {
		super(map);
	      parameter = new ParameterConfig((Map<String, Object>) getVal(KEY_PARAMETER_CONFIG));
	      
	}

    public String getName() {
        return getStringVal(KEY_WRITER_NAME);
    }

    public void setName(String name) {
        setStringVal(KEY_WRITER_NAME, name);
    }

    public ParameterConfig getParameter() {
        return parameter;
    }

    public void setParameter(ParameterConfig parameter) {
        this.parameter = parameter;
    }

    public static class ParameterConfig extends AbstractConfig {

        public ParameterConfig(Map<String, Object> map) {
            super(map);
        }

        
    }


}

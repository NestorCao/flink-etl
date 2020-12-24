package com.dtstack.flinkx.step;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.dtstack.flinkx.classloader.ClassLoaderManager;
import com.dtstack.flinkx.classloader.PluginUtil;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.StepConfig;
import com.dtstack.flinkx.writer.BaseDataWriter;

public class StepFactory {

	private StepFactory() {
		
	}
	 public static List<BaseStep> getSteps(DataTransferConfig config) {
	        try {
	        	List<StepConfig> stepConfigList = config.getJob().getContent().get(0).getSteps();
	            List<BaseStep> steps = new ArrayList<>();
	        	for(StepConfig stepConfig: stepConfigList) {
	            	String stepName = stepConfig.getName();
		        	steps.add(newStepInstance(stepName,stepConfig));
	            }
	        	return steps;
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }
	 private static BaseStep newStepInstance(String stepName,StepConfig stepConfig) {
		 if(stepName.equals("convertion")) {
			return  new ConvertionStep(stepConfig);
		 }
		 return null;
	 }
}

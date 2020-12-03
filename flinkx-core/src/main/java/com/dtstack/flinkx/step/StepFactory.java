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
	            	String pluginName = stepConfig.getName();
		        	String pluginClassName = PluginUtil.getPluginClassName(pluginName);
		            Set<URL> urlList = PluginUtil.getJarFileDirPath(pluginName, config.getPluginRoot(), config.getRemotePluginPath());
                    steps.add( 
		            ClassLoaderManager.newInstance(urlList, cl -> {
		                Class<?> clazz = cl.loadClass(pluginClassName);
		                Constructor constructor = clazz.getConstructor(DataTransferConfig.class);
		                return (BaseStep)constructor.newInstance(stepConfig);
		            })
		            );
	                
	            }
	        	return steps;
	        } catch (Exception e) {
	            throw new RuntimeException(e);
	        }
	    }
}

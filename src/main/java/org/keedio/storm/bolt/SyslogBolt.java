package org.keedio.storm.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import org.scoja.client.Syslogger;
import org.scoja.client.UDPSyslogger;
import org.scoja.client.LoggingException;
import org.scoja.client.ReusingTCPSyslogger;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.opencsv.CSVReader;

public class SyslogBolt extends BaseRichBolt {

    private static final long serialVersionUID = 8831211985061474513L;

    public static final Logger LOG = LoggerFactory
            .getLogger(SyslogBolt.class);

    private String host;
    private int port;
    private OutputCollector collector;
    private Syslogger syslogLogger;
    private Map<String,Syslogger> sysLoggerList = new HashMap<String,Syslogger>();
    private Map<String,Map<String,String>> hostPortEndpointMap = new HashMap<String,Map<String,String>>();
    private List<String> csvKeys;


    @Override
    public void cleanup() {
        try {
        	if (syslogLogger != null)
        	    syslogLogger.close();
        }
        catch (LoggingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    @SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        

		try {
			loadBoltProperties(stormConf);

    		if (hostPortEndpointMap.isEmpty() && host == null){
    			throw new ConfigurationException("Destination host or csvFilePath must be specified in properties file");
    		}
    		else if (!hostPortEndpointMap.isEmpty()){
    			for (String key : hostPortEndpointMap.keySet()) {
    				String host = hostPortEndpointMap.get(key).get("host");
    				String port = hostPortEndpointMap.get(key).get("port");
    				String protocol = hostPortEndpointMap.get(key).get("protocol");
    				if (protocol.equals("TCP"))
    					sysLoggerList.put(key, new ReusingTCPSyslogger(host, Integer.parseInt(port)));
    				else if (protocol.equals("UDP"))
    					sysLoggerList.put(key, new UDPSyslogger(host, Integer.parseInt(port)));
    				else
    					throw new ConfigurationException("Not valid protocol in file. Programmer review sanity checking!");
    			}
    		}else{
    			syslogLogger = new ReusingTCPSyslogger(host, port);
    		}
    			
			this.collector = collector;
		} catch (IOException | ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
    	return null;
    }

    @Override
    public void execute(Tuple input) {
    	 
	    try {
	    	if (sysLoggerList.isEmpty()){
				syslogLogger.log(input.getString(0));
				syslogLogger.flush();
	    	}else{
	    		final String json = input.getString(0);
	    		final Gson gson = new Gson();
	    	    Properties properties = gson.fromJson(json, Properties.class);
	    	    
	    	    final String extraDataJson = properties.getProperty("extraData");
	    	    properties = gson.fromJson(extraDataJson, Properties.class);
	    	    String searchKey = "";
	    	    for (int i=0; i<csvKeys.size(); i++){
	    	    	searchKey.concat(properties.getProperty(csvKeys.get(i)) + "_");
	    	    }
	    	    searchKey = searchKey.substring(0, searchKey.length()-1);
	    	    
	    	    sysLoggerList.get(searchKey).log(input.getString(0));
	    	    sysLoggerList.get(searchKey).flush();
	    	}
		} catch (LoggingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    collector.ack(input);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private void loadBoltProperties(Map stormConf) throws IOException, ConfigurationException {
    	
	    	host = (String) stormConf.get("bolt.syslog.host");
	    	port = Integer.parseInt((String) stormConf.getOrDefault("bolt.syslog.port","514"));
	    	String csvFilePath = (String) stormConf.get("bolt.syslog.csvFilePath");
	    	
	    	if (csvFilePath != null){
	    		loadCsvFileContent(csvFilePath);
	    	}
    }

	private void loadCsvFileContent(String filePath) throws IOException, ConfigurationException {
    	 

		Path pt=new Path(filePath);
		FileSystem fs = FileSystem.get(new Configuration());
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		CSVReader reader = new CSVReader(br);
            
		try{
            
            String line[]=reader.readNext();
            // hostPortEndpointMap -> Key = key1_key2_key3_key4  Value = host:hostname port:portNumber protocol:udp|tcp 
            
            if (line != null){
            	
            	// Get csv file Headers and Keys for search
            	List<String> csvHeader = new ArrayList<String>(Arrays.asList(line));
            	csvKeys = new ArrayList<>();

            	for (int i=0;i<csvHeader.size();i++){
            		if (csvHeader.get(i).contains("KEY_")){
            			csvKeys.add(csvHeader.get(i));
            		}
            	}
            	
            	// Check if headers contains host, port and protocol
            	if (!(csvHeader.contains("host") && csvHeader.contains("port") && csvHeader.contains("protocol"))){
            		throw new ConfigurationException("Bad csvFile: host, port and protocol must be in headers");
            	}
            	
            	// Check if at least one key exist
            	if (csvKeys.size() == 0)
            		throw new ConfigurationException("Bad csvFile: At least one KEY_ must be defined");
            	
            	
            	// Fill List with all lines, each line is a Map with key=header value=value
            	List<Map<String,String>> fileLines = new ArrayList<Map<String,String>>();
            	
            	int currentLinePosition = 2;
            	line=reader.readNext();
            	
            	// At least one line with values must be present in csv File
            	if (line == null){
            		throw new ConfigurationException("Bad csvFile: At least one line withvalues must exists");
            	}            	
            	
            	while (line != null){
                    // Check line sanity
                    if (csvHeader.size() != line.length){
                    	throw new ConfigurationException("Bad csvFile: Line in position " 
                    			+ currentLinePosition + " has " + line.length + " and " + csvHeader.size() + " expected.");
                    }
                    
                    Map<String,String> auxMap = new HashMap<String,String>();
                    
                    for (int i=0; i<csvHeader.size(); i++){
                    	auxMap.put(csvHeader.get(i), line[i]);
                    }
                    
                    fileLines.add(auxMap);
                    line=reader.readNext();
                    currentLinePosition++;
            	}
            	
            	// Build Map with searching key (key1_key2_key3) and map with host, port and protocol
            	for (int i=0; i<fileLines.size();i++){
            		
            		Map<String,String> currentLine = fileLines.get(i);
            		
            		String hostPortProtocolKey = currentLine.get(csvKeys.get(0));
            		Map<String,String> hostPortProtocolMap = new HashMap<>(3);
            		
            		for (int j=1; j<csvKeys.size(); j++){
            			hostPortProtocolKey.concat("_" + currentLine.get(csvKeys.get(j)));
            		}
            		
            		// Check sanity of key and host port and protocol
            		if (hostPortEndpointMap.containsKey(hostPortProtocolKey))
            			throw new ConfigurationException("CsvFile contains duplicate in line (" + i+1 + ")");
            		
            		String host = currentLine.get("host");
            		String port = currentLine.get("port");
            		String protocol = currentLine.get("protocol");
            		
            		// Check sanity port and protocol
            		if (Integer.parseInt(port) < 1 || Integer.parseInt(port) > 65535)
            			throw new ConfigurationException("Port must be between 1 and 65535. Check line (" + i+1 + ")");
            		if (!protocol.equals("TCP") && !protocol.equals("UDP"))
            			throw new ConfigurationException("Protocol must be TCP or UDP. Check line (" + i+1 + ")");
            		            		
            		hostPortProtocolMap.put("host", host);
            		hostPortProtocolMap.put("port", port); 
            		hostPortProtocolMap.put("protocol", protocol);
            		
            		hostPortEndpointMap.put(hostPortProtocolKey,hostPortProtocolMap);
            	}
            }
            else{
            	throw new ConfigurationException("Csv file empty!!");
            }
		}finally{
			reader.close();
		}
	} 
}

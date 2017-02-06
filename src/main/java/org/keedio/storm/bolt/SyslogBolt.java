package org.keedio.storm.bolt;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.codehaus.jackson.map.ObjectMapper;
import org.scoja.client.Syslogger;
import org.scoja.client.UDPSyslogger;
import org.scoja.client.LoggingException;
import org.scoja.client.ReusingTCPSyslogger;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVReader;

public class SyslogBolt extends BaseRichBolt {

    private static final long serialVersionUID = 8831211985061474513L;

    public static final Logger LOG = LoggerFactory
            .getLogger(SyslogBolt.class);

    private String host, protocol;
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
        
		try {
			loadBoltProperties(stormConf);
			syslogConnect();
			this.collector = collector;
		} catch (IOException | URISyntaxException e) {
			throw new RuntimeException(e.getMessage());
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
    	
    	String inputMessage = new String(input.getBinary(0));
    	
	    try {
	    	if (sysLoggerList.isEmpty()){
				syslogLogger.log(inputMessage);
				syslogLogger.log(System.getProperty("line.separator"));
				syslogLogger.flush();
	    	}else{
	    		if (!inputMessage.isEmpty()){
		    	    
		    	    HashMap<String,Object> inputJson = new ObjectMapper().readValue(inputMessage, HashMap.class);		    	    
		    	    HashMap<String,String> extraData = (HashMap<String, String>) inputJson.get("extraData");
		    	    
		    	    String searchKey = "";
		    	    for (int i=0; i<csvKeys.size(); i++){
		    	    	searchKey += extraData.get(csvKeys.get(i));
		    	    	if (i <  csvKeys.size()-1){
		    	    		searchKey += "_";
		    	    	}
		    	    }
		    	    if (sysLoggerList.containsKey(searchKey)){
			    	    sysLoggerList.get(searchKey).log(inputMessage);
			    	    sysLoggerList.get(searchKey).log(System.getProperty("line.separator"));
			    	    sysLoggerList.get(searchKey).flush();
		    	    }else{
		    	    	LOG.error("Search key: " + searchKey + " .Not found in csv file");
		    	    }
	    		}
	    	}
		} catch (LoggingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    collector.ack(input);
    }

	private void loadBoltProperties(final Map<String, String> stormConf) throws IOException, ConfigurationException, URISyntaxException {
    	
	    	host = (String) stormConf.get("bolt.syslog.host");
	    	Object portObj = stormConf.get("bolt.syslog.port");
	    	
	    	if (portObj == null)
	    		port = 514;
	    	else{
	    		port = Integer.parseInt((String) portObj);
	    		if (port < 1 || port > 65535)
	    			throw new ConfigurationException("Port must be between 1 and 65535");
	    	}
	    	
	    	if (stormConf.get("bolt.syslog.protocol") == null)
	    		protocol = "TCP";
	    	else {
		    	protocol = ((String) stormConf.get("bolt.syslog.protocol")).toUpperCase();
		    	if (!(protocol.equals("TCP") || protocol.equals("UDP"))){
		    		throw new ConfigurationException("Protocol must be TCP or UDP");
		    	}
	    	}
	    	
	    	final String csvFilePath = (String) stormConf.get("bolt.syslog.csvFilePath");    	
	    	final String hdfsRoot = (String) stormConf.get("bolt.syslog.hdfsRoot");
	    	
	    	if (csvFilePath != null && csvFilePath.length() > 0){
	    		loadCsvFileContent(csvFilePath,hdfsRoot);
	    	}
    }

	private void loadCsvFileContent(final String filePath, final String hdfsRoot) throws IOException, ConfigurationException, URISyntaxException {
    	 
		final Path path = new Path(filePath);

		final InputStreamReader streamReader;
		
		int currentLinePos = 0;

		if (hdfsRoot != null){
			final DistributedFileSystem dFS = new DistributedFileSystem() {
				{
					initialize(new URI(hdfsRoot), new Configuration());
				}
			};
			streamReader = new InputStreamReader(dFS.open(path));
		}else{
			final File file = new File(filePath);
			streamReader = new InputStreamReader(new FileInputStream(file));
		}
		
		final CSVReader reader = new CSVReader(streamReader);
		          
		try{        
            String line[]=reader.readNext();
            // hostPortEndpointMap -> Key = key1_key2_key3_key4  Value = host:hostname port:portNumber protocol:udp|tcp 
            
            if (line != null){        	
            	// Get csv file Headers and Keys for search
            	List<String> csvHeader = new ArrayList<String>(Arrays.asList(line));
            	csvKeys = new ArrayList<>();

            	for (int i=0;i<csvHeader.size();i++){
            		if (csvHeader.get(i).startsWith("KEY_")){
            			csvKeys.add(csvHeader.get(i).substring(4));
            		}
            	}
            	
            	// Check if at least one key exist
            	if (csvKeys.size() == 0)
            		throw new ConfigurationException("Bad csvFile: At least one KEY_ must be defined");
            	
            	// Check if headers contains host, port and protocol
            	if (!(csvHeader.contains("host") && csvHeader.contains("port") && csvHeader.contains("protocol"))){
            		throw new ConfigurationException("Bad csvFile: host, port and protocol must be in headers");
            	}
            	
            	
            	// Fill List with all lines, each line is a Map with key=header value=value
            	List<Map<String,String>> fileLines = new ArrayList<Map<String,String>>();
            	
            	int currentLinePosition = 2;
            	line=reader.readNext();
            	
            	// At least one line with values must be present in csv File
            	if (line == null){
            		throw new ConfigurationException("Bad csvFile: At least one line with values must exists");
            	}            	
            	
            	while (line != null){
                    // Check line sanity
                    if (csvHeader.size() != line.length){
                    	throw new ConfigurationException("Bad csvFile: Line (" 
                    			+ currentLinePosition + ") has " + line.length + " fields and " + csvHeader.size() + " are expected.");
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
            	for (currentLinePos=0; currentLinePos<fileLines.size();currentLinePos++){
            		
            		Map<String,String> currentLine = fileLines.get(currentLinePos);
            		            		
            		String hostPortProtocolKey = currentLine.get("KEY_"+csvKeys.get(0));
            		for (int j=1; j<csvKeys.size(); j++){
            			hostPortProtocolKey += "_" + currentLine.get("KEY_"+csvKeys.get(j));
            		}
            		
            		// Check sanity of key and host port and protocol
            		if (hostPortEndpointMap.containsKey(hostPortProtocolKey))
            			throw new ConfigurationException("CsvFile contains duplicate in line (" 
            											+ (currentLinePos+2) + ")");
            		
            		String host = currentLine.get("host");
            		String port = currentLine.get("port");
            		String protocol = currentLine.get("protocol").toUpperCase();
            		
            		// Check sanity port and protocol
            		if (Integer.parseInt(port) < 1 || Integer.parseInt(port) > 65535)
            			throw new ConfigurationException("Port must be between 1 and 65535. Check line (" 
            											+ (currentLinePos+2) + ")");
            		if (!protocol.equals("TCP") && !protocol.equals("UDP"))
            			throw new ConfigurationException("Protocol must be TCP or UDP. Check line (" 
            											 + (currentLinePos+2) + ")");   
            		
            		Map<String,String> hostPortProtocolMap = new HashMap<>(3);
            		
            		hostPortProtocolMap.put("host", host);
            		hostPortProtocolMap.put("port", port); 
            		hostPortProtocolMap.put("protocol", protocol);
            		
            		hostPortEndpointMap.put(hostPortProtocolKey,hostPortProtocolMap);
            	}
            }
            else{
            	throw new ConfigurationException("Csv file empty!!");
            }
		}catch (NumberFormatException e){
			throw new RuntimeErrorException(new Error(),"Number Format Exception: Check ports in csv file, line (" + (currentLinePos+2) +")");
		}finally{
			reader.close();
		}
}

    private void syslogConnect() throws ConfigurationException, NumberFormatException, UnknownHostException, SocketException {
    	
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
			if (protocol.equals("TCP"))
				syslogLogger = new ReusingTCPSyslogger(host, port);
			else if (protocol.equals("UDP"))
				syslogLogger = new UDPSyslogger(host, port);
			else
				throw new ConfigurationException("Not valid protocol in file. Programmer review sanity checking!");
			
		}		
	}
}

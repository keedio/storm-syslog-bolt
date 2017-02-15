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

import org.scoja.client.Syslogger;
import org.scoja.client.UDPSyslogger;
import org.scoja.client.LoggingException;
import org.scoja.client.ReusingTCPSyslogger;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.conf.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.opencsv.CSVReader;

import static org.keedio.storm.bolt.SyslogBoltProperties.*;

public class SyslogBolt extends BaseRichBolt {

    private static final long serialVersionUID = 8831211985061474513L;

    public static final Logger LOG = LoggerFactory
            .getLogger(SyslogBolt.class);

    private String host, protocol;
    private boolean isEnriched = false;
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
    	return super.getComponentConfiguration();
    }

    @SuppressWarnings("unchecked")
	@Override
    public void execute(Tuple input) {
    	
    	Map<String, Object> inputJson;
    	Map<String,String> extraData = null;
    	String message = new String(input.getBinary(0));
    	
    	if (isEnriched){
        	try {
				Gson gson = new GsonBuilder().create();
				inputJson = gson.fromJson(message,Map.class);    	    
	    	    extraData = (Map<String, String>) inputJson.get("extraData");
	    		message = (String) inputJson.get("message");
        	} catch (JsonSyntaxException e) {
        		collector.reportError(e);
        		collector.ack(input);
    			e.printStackTrace();
        	}
    	}

    	if (sysLoggerList.isEmpty()){
    		try{
    			syslogLogger.log(message);
    			syslogLogger.flush();
    		} catch (LoggingException e) {
    			collector.fail(input);
    			reconnectSyslogConnection(syslogLogger);
    			e.printStackTrace();
    		}
    	}else if (isEnriched){
    		if (!message.isEmpty()){
    	    	String searchKey = "";
	    	    
	    	    for (int i=0; i<csvKeys.size(); i++){
	    	    	searchKey += extraData.get(csvKeys.get(i));
	    	    	if (i <  csvKeys.size()-1){
	    	    		searchKey += "_";
	    	    	}
	    	    }
	    	    try{
		    	    if (sysLoggerList.containsKey(searchKey)){
			    	    sysLoggerList.get(searchKey).log(message);
			    	    sysLoggerList.get(searchKey).flush();
		    	    }else{
		    	    	LOG.error("Search key: " + searchKey + " .Not found in csv file");
		    	    }
                } catch (LoggingException e) {
                    collector.reportError(e);
                    collector.fail(input);
                    LOG.error("Connection with server lost");
                    reconnectSyslogConnection(sysLoggerList.get(searchKey));
                }
            }
        }else{
            throw new RuntimeException("To use csv endopoint file, syslog.bolt.enriched must be setted to true");
        }
        
        collector.ack(input);
    }

    private void reconnectSyslogConnection(Syslogger syslogLogger) {
        
        int retryDelayMs = 1000;
        boolean connected = false;

        while (!connected) {
        
            try {
                syslogLogger.reset();
                connected=true;
            } catch (LoggingException e){
                try{
                    Thread.sleep(retryDelayMs);
                    if (retryDelayMs < 10000)
                        retryDelayMs += 1000;
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }
    }

    private void loadBoltProperties(final Map<String, String> stormConf) throws IOException, ConfigurationException, URISyntaxException {
        
            host = (String) stormConf.get(SYSLOG_BOLT_HOST);
            Object portObj = stormConf.get(SYSLOG_BOLT_PORT);
            
            if (stormConf.get(SYSLOG_BOLT_ENRICHED) != null && stormConf.get(SYSLOG_BOLT_ENRICHED).equals("true"))
                isEnriched=true;
            
            if (portObj == null)
                port = 514;
            else{
                port = Integer.parseInt((String) portObj);
                if (port < 1 || port > 65535)
                    throw new ConfigurationException("Port must be between 1 and 65535");
            }
            
            if (stormConf.get(SYSLOG_BOLT_PROTOCOL) == null)
                protocol = "TCP";
            else {
                protocol = ((String) stormConf.get(SYSLOG_BOLT_PROTOCOL)).toUpperCase();
                if (!(protocol.equals("TCP") || protocol.equals("UDP"))){
                    throw new ConfigurationException("Protocol must be TCP or UDP");
                }
            }
            
            if (stormConf.get(SYSLOG_BOLT_DYNAMIC_ENDPOINT) != null && stormConf.get(SYSLOG_BOLT_DYNAMIC_ENDPOINT).equals("true")){
            	final String csvFilePath = (String) stormConf.get(SYSLOG_BOLT_CSV_FILE_PATH);       
                final String hdfsRoot = (String) stormConf.get(SYSLOG_BOLT_HDFS_ROOT);
                if (csvFilePath != null && csvFilePath.length() > 0){
                    if (!isEnriched)
                        throw new ConfigurationException("To use csv endopoint file, syslog.bolt.enriched must be setted to true");
                    
                    loadCsvFileContent(csvFilePath,hdfsRoot);
                }
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
                    sysLoggerList.put(key, new ReusingTCPSyslogger(host, Integer.parseInt(port)).setTerminator(System.getProperty("line.separator")).setTag(""));
                else if (protocol.equals("UDP"))
                    sysLoggerList.put(key, new UDPSyslogger(host, Integer.parseInt(port)).setTerminator(System.getProperty("line.separator")).setTag(""));
                else
                    throw new ConfigurationException("Not valid protocol in file. Programmer review sanity checking!");
            }
        }else{
            if (protocol.equals("TCP"))
                syslogLogger = new ReusingTCPSyslogger(host, port).setTerminator(System.getProperty("line.separator")).setTag("");
            else if (protocol.equals("UDP"))
                syslogLogger = new UDPSyslogger(host, port).setTerminator(System.getProperty("line.separator")).setTag("");
            else
                throw new ConfigurationException("Not valid protocol in file. Programmer review sanity checking!");
            
        }       
    }
}

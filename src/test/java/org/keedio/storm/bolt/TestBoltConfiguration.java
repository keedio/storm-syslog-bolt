package org.keedio.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.keedio.storm.bolt.ConfigurationException;
import org.keedio.storm.bolt.SyslogBolt;

import static org.keedio.storm.bolt.SyslogBoltProperties.*;

public class TestBoltConfiguration {
	
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();
	
	// PORT tests
	@Test(expected = NumberFormatException.class)
	public void notValidPort(){
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>(1);
		stormConf.put(SYSLOG_BOLT_PORT, "badPort");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void portGreaterThanMaximumPort(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Port must be between 1 and 65535");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>(1);
		stormConf.put(SYSLOG_BOLT_PORT, "70000");
		
		sb.prepare(stormConf, null, null);		
	}
	
	// HOST tests
	@Test
	public void hostNotSet(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Destination host or csvFilePath must be specified in properties file");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>(1);
		stormConf.put(SYSLOG_BOLT_PROTOCOL, "tcp");
		
		sb.prepare(stormConf, null, null);		
	}
	
	// PROTOCOL tests
	@Test
	public void incorrectProtocol(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Protocol must be TCP or UDP");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>(1);
		stormConf.put(SYSLOG_BOLT_HOST, "localhost");
		stormConf.put(SYSLOG_BOLT_PROTOCOL, "badProtocol");
		
		sb.prepare(stormConf, null, null);
	}
	
	
//	@Test
//	public void csvNotExistsInHDFSSetInConfig(){
//		
//		expectedEx.expect(RuntimeException.class);
//		expectedEx.expectMessage("Incomplete HDFS URI, no host: badURI");
//		
//		SyslogBolt sb = new SyslogBolt();
//		
//		Map<String,String> stormConf = new HashMap<String,String>(1);
//		stormConf.put("bolt.syslog.csvFilePath", "resources/emptyCsv.csv");
//		stormConf.put("bolt.syslog.hdfsRoot", "badURI");
//		
//		sb.prepare(stormConf, null, null);		
//	}
	
	
	
	/*
	@Test
	public void connectToDefaultPortDefaultProtocol(){
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>(1);
		
		sb.prepare(stormConf, null, null);		
	}
	*/
	
//	@Test(expected = FileNotFoundException.class)
//	public void csvPathNotExists(){
//		SyslogBolt sb = new SyslogBolt();
//		
//		Map<String,String> stormConf = new HashMap<String,String>(1);
//		stormConf.put("bolt.syslog.csvFilePath", "/tmp/12gs5342qjh/thisFileMustNotExists.csv");
//		
//		sb.prepare(stormConf, null, null);		
//	}
//	
	
	
//	@Test
//    public void testTCPBolt() {
//		
//		MkClusterParam mkClusterParam = new MkClusterParam();
//	    mkClusterParam.setSupervisors(1);
//	    Config daemonConf = new Config();
//	    daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
//	    
//	    mkClusterParam.setDaemonConf(daemonConf);
//	    
//	    TestJob testJob = new TestJob() {
//            @SuppressWarnings("rawtypes")
//			@Override
//            public void run(ILocalCluster cluster) throws IOException {
//                TopologyBuilder builder = new TopologyBuilder();
//
//                builder.setSpout("fakeKafkaSpout", new FeederSpout(new Fields("field1")));
//
//                builder.setBolt("TCPBolt", new SyslogBolt())
//                .shuffleGrouping("fakeKafkaSpout");
//
//                StormTopology topology = builder.createTopology();
//
//                MockedSources mockedSources = new MockedSources();
//
//                //Our spout will be processing this values.
//                mockedSources.addMockData("fakeKafkaSpout",new Values("fieldValue1".getBytes()));
//
//                // prepare the config
//                Config conf = new Config();
//                conf.setNumWorkers(1);
//                conf.put("tcp.bolt.port", "7657");
//                conf.put("tcp.bolt.host", "localhost");
//                conf.put(Config.NIMBUS_SEEDS, Collections.singletonList("localhost"));
//                conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
//                
//
//                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
//                completeTopologyParam.setMockedSources(mockedSources);
//                completeTopologyParam.setStormConf(conf);
//               
//               Future<String> future = startServer();
//
//                Testing.completeTopology(cluster, topology, completeTopologyParam);
//
//                // check whether the result is right
//                try {
//					Assert.assertEquals("fieldValue1", future.get());
//				} catch (InterruptedException | ExecutionException e) {
//					e.printStackTrace();
//				}
//            }
//	    };
//	
//	    Testing.withSimulatedTimeLocalCluster(mkClusterParam, testJob);
//    }
//	
//	@SuppressWarnings({ "rawtypes", "unchecked" })
//	public Future<String> startServer() {
//        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);
//
//        Callable<String> serverTask = new Callable() {
//            @Override
//            public String call() {
//            	BufferedReader in;
//                try {
//                    ServerSocket serverSocket = new ServerSocket(7657);
//                    Socket clientSocket = serverSocket.accept();
//                    in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//                    serverSocket.close();
//                    return in.readLine();
//                    } catch (IOException e) {
//                    System.err.println("Unable to process client request");
//                    e.printStackTrace();
//                }
//                return "";
//            }
//        };
//        return clientProcessingPool.submit(serverTask);
//    }

}


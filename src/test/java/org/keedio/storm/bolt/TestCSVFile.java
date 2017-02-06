package org.keedio.storm.bolt;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.keedio.storm.bolt.ConfigurationException;
import org.keedio.storm.bolt.SyslogBolt;

public class TestCSVFile {
	
	@Rule
	public ExpectedException expectedEx = ExpectedException.none();
	
    // CSV tests
	@Test
	public void noHostPortAndProtocolInCSV(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Bad csvFile: host, port and protocol must be in headers");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/noHost.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void hdfsRootNotValid(){
		
		expectedEx.expect(RuntimeException.class);
		expectedEx.expectMessage("Incomplete HDFS URI, no host: badURI");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/dummyFile.csv");
		stormConf.put("bolt.syslog.hdfsRoot", "badURI");
		
		sb.prepare(stormConf, null, null);		
	}

	
	@Test
	public void localCSVfileNotExists(){
		
		expectedEx.expect(RuntimeException.class);
		expectedEx.expectMessage("No such file or directory");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/notExists.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void emptyCSV(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Csv file empty!!");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/emptyFile.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void noHeaderInCSV(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Bad csvFile: At least one KEY_ must be defined");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/noHeader.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void noKeyInHeaderCSV(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Bad csvFile: At least one KEY_ must be defined");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/noKeyInHeader.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void onlyHeaderInCSV(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Bad csvFile: At least one line with values must exists");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/onlyHeader.csv");
		
		sb.prepare(stormConf, null, null);		
	}
		
	@Test
	public void numberOfHeadersAndValuesNotMatch(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("Bad csvFile: Line (2) has 3 fields and 4 are expected.");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/headerAndValuesNotMatch.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void csvDuplicateKey(){
		
		expectedEx.expect(ConfigurationException.class);
		expectedEx.expectMessage("CsvFile contains duplicate in line (3)");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/duplicateKey.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void badPortFormat(){
		
		expectedEx.expect(RuntimeException.class);
		expectedEx.expectMessage("Number Format Exception: Check ports in csv file, line (2)");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/badPort.csv");
		
		sb.prepare(stormConf, null, null);		
	}
	
	@Test
	public void badPortRange(){
		
		expectedEx.expect(RuntimeException.class);
		expectedEx.expectMessage("Port must be between 1 and 65535. Check line (2)");
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		stormConf.put("bolt.syslog.csvFilePath", "src/test/resources/badPortRange.csv");
		
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
//		Map<String,String> stormConf = new HashMap<String,String>();
//		stormConf.put("bolt.syslog.csvFilePath", "resources/emptyCsv.csv");
//		stormConf.put("bolt.syslog.hdfsRoot", "badURI");
//		
//		sb.prepare(stormConf, null, null);		
//	}
	
	
	
	/*
	@Test
	public void connectToDefaultPortDefaultProtocol(){
		
		SyslogBolt sb = new SyslogBolt();
		
		Map<String,String> stormConf = new HashMap<String,String>();
		
		sb.prepare(stormConf, null, null);		
	}
	*/
	
//	@Test(expected = FileNotFoundException.class)
//	public void csvPathNotExists(){
//		SyslogBolt sb = new SyslogBolt();
//		
//		Map<String,String> stormConf = new HashMap<String,String>();
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


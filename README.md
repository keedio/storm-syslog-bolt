#storm-syslog-bolt
##Description

This bolt receives a byte array and send it using syslog format, to configured host and port.
TCP and UDP protocols are supported.

## Static host, port and protocol
In this mode the bolt will send all the input data to the configured host and port.

|property|default|description|
|--------|-------|-----------|
|<b>syslog.bolt.host</b>| | Ip or hostname to send messages (mandatory) |
|syslog.bolt.port|514| Endpoint port |
|syslog.bolt.protocol|tcp| tcp or udp supported |

## Dinamic host, port and protocol
The bolt can dinamically send the messages to a configured host and port in an external csv file.

The input message must be in enriched Keedio format like:
```json
{
 "extraData":{"source_host": "host-1", "source_type": "system", "date": "2015-04-23", "time": "07:16:08"},
 "message": "the original body string"
}
```

### Property file configuration

|property|default|description|
|--------|-------|-----------|
|syslog.bolt.enriched|false| This property must be set to <b>true</b> |
|syslog.bolt.csvFilePath||The full file path to Csv enpoints file |
|syslog.bolt.hdfsRoot||If this property is set, the csv file will be searched in HDFS |

Example:
```
syslog.bolt.enriched=true
syslog.bolt.hdfsRoot=hdfs://namenode1:8020
syslog.bolt.csvFilePath=/user/storm/syslogEndpoints.csv
```

### Endopoint Csv File
The format of the file is like:
```csv
KEY_extraData-key1,KEY_extraData-key2,host,port,protocol
value11,value12,value13,host-1,5140,tcp
```

Using the values in extraData, the message will be sent to the specified host, port and protocol

### Example
Usin the following endpoints CSV File:
```csv
KEY_source_host,KEY_source_type,host,port,protocol
host-1,system,192.168.1.4,5140,tcp
host-2,user,192.168.1.5,5141,udp
```
The input message:
```json
{
 "extraData":{"source_host": "host-1", "source_type": "system", "date": "2015-04-23", "time": "07:16:08"},
 "message": "the original body string"
}
```
will be send to 192.168.1.4:5140 using tcp.

and the input message:
```json
{
 "extraData":{"source_host": "host-2", "source_type": "user", "date": "2015-04-23", "time": "07:16:08"},
 "message": "the original body string"
}
```
will be send to 192.168.1.5:5141 using udp


## Compilation
Use maven
````
mvn clean package
```

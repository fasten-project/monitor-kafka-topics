# Monitor Kafka topics
A generic Flink job to monitor (unique) messages in a Kafka topic and publishes to InfluxDB.

## Usage
```bash
Monitor Kafka Topics to InfluxDB
Usage: MonitorKafka [options]

  -b, --brokers <broker1>,<broker2>,...
                           A set of Kafka brokers to connect to.
  -t, --topic <topic>      The topic to read from.
  -k, --key <key1>,<key2>,...
                           The keys to aggregate on.
  -t, --emit_time <time>   The time (in seconds) to emit statistics to InfluxDB.
  --influx_host <host>     InfluxDB hostname.
  --influx_port <port>     InfluxDB port.
  --influx_database <database>
                           InfluxDB database.
  --parallelism <value>    The amount of parallel workers for Flink.
  --backendFolder <value>  Folder to store checkpoint data of Flink.
```

## To Deploy
Build a jar file using `sbt assembly` and then deploy onto a Flink cluster using the Python scripts in the `deployment` folder.
```bash
python3 overview.py FLINK_ADDRESS
```
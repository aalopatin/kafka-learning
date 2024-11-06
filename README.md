# Console commands

##### Create topic
```shell
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic temperature
```

##### Delete topic
```shell
kafka-topics.bat --bootstrap-server localhost:9092 --delete --topic temperature
```

##### Create topic with config
```shell
kafka-topics.bat --bootstrap-server localhost:9092 --create --topic short-messages --config max.message.bytes=100
```
```shell
kafka-configs.bat --bootstrap-server localhost:9092 --alter --add-config 'producer_byte_rate=300' --entity-name producerWithQuota --entity-type clients
```
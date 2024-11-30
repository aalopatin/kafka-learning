### Run this script from parent root directory
```shell
$env:CLASSPATH="target/libs/producer-interceptor.jar"
kafka-console-producer.bat --bootstrap-server localhost:9092 --topic interceptor-topic --producer.config producer-interceptor/producer.config
```
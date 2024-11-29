# Run this script from parent root directory
$env:CLASSPATH="target/libs/producer-interceptor.jar"
kafka-console-producer.bat --bootstrap-server localhost:9092 --topic interceptor-topic --producer.config producer-interceptor/cmd/producer.config
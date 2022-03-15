
FROM morgel/flink:1.14.0-bin-scala_2.11-java_11

COPY processor/target/jobs-1.0.jar /opt/flink/usrlib/

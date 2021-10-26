FROM openjdk:8 as builder

RUN apt-get update && apt-get install -y git
FROM maven:3.5-jdk-8 AS build
RUN git clone https://github.com/yarongilor/kraken-dynamo.git -b mvn_docker
RUN cd kraken-dynamo; git submodule update --init --recursive; mvn install
RUN cd kraken-dynamo/syncer; mvn package

FROM openjdk:8 as app
RUN echo 'networkaddress.cache.ttl=0' >> $JAVA_HOME/jre/lib/security/java.security
RUN echo 'networkaddress.cache.negative.ttl=0' >> $JAVA_HOME/jre/lib/security/java.security
COPY java.policy $JAVA_HOME/jre/lib/security/java.policy

COPY --from=build /kraken-dynamo /kraken-dynamo

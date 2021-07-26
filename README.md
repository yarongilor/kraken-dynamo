# kraken-dynamo
originally base on this example:
https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/Streams.KCLAdapter.Walkthrough.html

Re-written to use only Amazon SDK v2 async dynamo streams interfaces, to bypass SDK v1/kinesis poor 
handling of large number of stream shards.

This project is split into two modules, since it requires a patched (forked) version of the scylla
dynamo load balancer. Actual program is in the "syncer" module.

### example of how to run it
```
docker run -d -p 8080:8080 -p 9042:9042 scylladb/scylla-nightly:4.4.dev-0.20201217.a60c81b61 --alternator-port 8080 --alternator-write-isolation always --experimental 1
# wait for scylla to be ready

# run the KCL example
# (in top level dir)
maven install
#in "syncer" module
maven exec:java -Dexec.args="-e http://localhost:8080 -t usertable -k 1000"

# (or to build a fat jar with all dependecies built in - in top level)
maven package
# will generate a syncer/target/kraken-dynamo-syncer-<version>-SNAPSHOT-jar-with-dependencies
# that can be executed with simple java -jar

# after ~30sec when tables created run ycsb


# running with local ycsb
python2.7 ./bin/ycsb load dynamodb  -P workloads/workloada -P dynamodb.properties -p recordcount=1000 -p insertorder=uniform -p insertcount=1000 -p fieldcount=2 -p fieldlength=50 -s

# running with docker ycsb [keep in mind you localhost won't work inside docker, and you need a real address of you network interface]
echo "accessKey=" > aws_dummy
echo "secretKey=" >> aws_dummy
docker run -v `pwd`/aws_dummy:/YCSB/aws_dummy -w /YCSB scylladb/hydra-loaders:ycsb-jdk8-20200326 ./bin/ycsb load dynamodb  -P workloads/workloada \
 -p dynamodb.endpoint=http://192.168.122.1:8080 -p dynamodb.primaryKey=p -p dynamodb.awsCredentialsFile=aws_dummy \
 -p recordcount=10000 -p insertorder=uniform -p insertcount=1000 -p fieldcount=2 -p fieldlength=50 -s


# when you want to clear it out and try again
cqlsh> DROP KEYSPACE "alternator_streams-demo"; DROP KEYSPACE "alternator_streams-demo-dest";
```

###
```bash
export KCL_DOCKER_IMAGE=scylladb/hydra-loaders:kcl-jdk8-$(date +'%Y%m%d')
docker build . -t ${KCL_DOCKER_IMAGE}
docker push ${KCL_DOCKER_IMAGE}
echo "${KCL_DOCKER_IMAGE}" > image
```

### TODOs

* [x] - dockerize it for usage in SCT


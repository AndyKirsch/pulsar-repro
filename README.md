A demonstration in behavior changes between the pulsar 2.10 client and the pulsar 2.11 client when mixing regex consumers and partitions. Assumes a running pulsar cluster locally via docker with a `9090:8080` port mapping.

Running
`sbt oldVersion/IntegrationTest/test`
and
`sbt newVersion/IntegrationTest/test`
to run 2.10 and 2.11 respectively. 

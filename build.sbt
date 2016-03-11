name := "RedisMonitorServer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.flume" % "flume-ng-core" % "1.6.0" % "provided"

libraryDependencies += "redis.clients" % "jedis" % "2.8.0"

mainClass in assembly := Some("com.mls.flume.monitor.LogCollect")
    
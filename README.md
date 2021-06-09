Simple program for testing insert performance between XCC, DMSDK, and the Bulk API.

First deploy the small ml-gradle project via:

```
./gradlew -i mlDeploy
```

You can then run the PerformanceTester program:

```
./gradlew testPerformance
```

The logging from this task will show long each approach takes. 

You can provide a different host/username/password by creating gradle-local.properties (which is gitignore'd) and 
defining properties in that file - e.g.

```
mlHost=some-remote-host
mlUsername=some-user
mlPassword=some-password
```

The properties in gradle-local.properties will take precedence over this in gradle.properties, both when deploying and 
running the performance test. 

You can also adjust the properties via Gradle's -P flag, which allows you to easily adjust the parameters of the 
performance test:

```
./gradlew testPerformance -PbatchCount=200 -PbatchSize=200 -PthreadCount=16 -Piterations=5
```

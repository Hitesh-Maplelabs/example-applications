# Example Streaming Application (Flink Streaming API)

## Overview

The example streaming application shows an example of an application that can be deployed using the PNDA Deployment Manager. (See the `platform-deployment-manager` project for details.)

This example uses the Flink Streaming APIs. When PNDA is configured to use HDP/CDH cluster, Flink is deployed by default.

The application is a tar file containing binaries and configuration files required to perform some stream processing.

This example application reads events from a network socket and performs basic counting analytics. To run the data source run the command `nc -l 9100` on the pnda edge node and then type some input into it.

The results are printed into the console output of the flink driver process. To view these, navigate to the log file via the Yarn Resource Manager UI or use the PNDA log server.


## Requirements

* [Maven](https://maven.apache.org/docs/3.0.5/release-notes.html) 3.0.5
* [Java JDK](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html) 1.8

## Build
Edit the `streaming-app/pom.xml` file with the correct dependencies.

To build the example applications use:

````
mvn clean package
````

This command should be run at the root of the repository and will build the application package. It will create a package file `flink-socket-streaming-example-app-{version}.tar.gz` in the `app-package/target` directory.

## Files in the package

- `application.properties`: config file used by the Flink Streaming scala application.
- `properties.json`: contains default properties that may be overriden at application creation time.

## Deploying the package and creating an application

The PNDA console can be used to deploy the application package to a cluster and then to create an application instance. The console is available on port 80 on the edge node.

To make the package available for deployment it must be uploaded to a package repository. The default implementation is an OpenStack Swift container. The package may be uploaded via the PNDA repository manager which abstracts the container used, or by manually uploading the package to the container.

Make sure to set `input_data_host` to the host that is running the `nc -l 9100` command that is used to provide data to the application.

Once the application is running, type some texts into the nc input and view the results in the flink's taskmanager.out on driver host. To view this file navigate to the taskmanager log via the Yarn Resource Manager UI or use the PNDA log server.




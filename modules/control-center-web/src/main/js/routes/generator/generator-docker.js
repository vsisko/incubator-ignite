/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Docker file generation entry point.
$generatorDocker = {};

// Generate Docker file for cluster.
$generatorDocker.clusterDocker = function (cluster, os) {
    if (!os)
        os = 'debian:8';

    return '# Start from a OS image.\n' +
        'FROM ' + os + '\n' +
        '\n' +
        '# Install tools.\n' +
        'RUN apt-get update && apt-get install -y --fix-missing \\\n' +
        '  wget \\\n' +
        '  dstat \\\n' +
        '  maven \\\n' +
        '  git\n' +
        '\n' +
        '# Install Oracle JDK.\n' +
        'RUN mkdir /opt/jdk\n' +
        '\n' +
        'RUN wget --header \"Cookie: oraclelicense=accept-securebackup-cookie\" \\\n' +
        '  http://download.oracle.com/otn-pub/java/jdk/7u79-b15/jdk-7u79-linux-x64.tar.gz\n' +
        '\n' +
        'RUN tar -zxf jdk-7u79-linux-x64.tar.gz -C /opt/jdk\n' +
        '\n' +
        'RUN rm jdk-7u79-linux-x64.tar.gz\n' +
        '\n' +
        'RUN update-alternatives --install /usr/bin/java java /opt/jdk/jdk1.7.0_79/bin/java 100\n' +
        '\n' +
        'RUN update-alternatives --install /usr/bin/javac javac /opt/jdk/jdk1.7.0_79/bin/javac 100\n' +
        '\n' +
        '# Sets java variables.\n' +
        'ENV JAVA_HOME /opt/jdk/jdk1.7.0_79/\n' +
        '\n' +
        '# Create working directory\n' +
        'WORKDIR /home\n' +
        '\n' +
        'RUN wget -O ignite.zip http://tiny.cc/updater/download_ignite.php && unzip ignite.zip && rm ignite.zip\n' +
        '\n' +
        'COPY *.xml /tmp/\n' +
        '\n' +
        'RUN mv /tmp/*.xml /home/$(ls)/config';
};

// For server side we should export Java code generation entry point.
if (typeof window === 'undefined') {
    module.exports = $generatorDocker;
}

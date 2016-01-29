#!/bin/bash
javac -classpath `hadoop classpath` *.java
jar cvf avg.jar *.class
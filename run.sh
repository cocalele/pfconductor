#!/bin/bash

#java -classpath ./out/production/jconductor:./lib/* com.netbric.s5.conductor.Main -c /etc/pureflash/s5.conf 
JCROOT=/root/workspace2/jconductor
java -classpath $JCROOT/out/production/jconductor:$JCROOT/lib/* com.netbric.s5.conductor.Main -c /etc/pureflash/s5.conf
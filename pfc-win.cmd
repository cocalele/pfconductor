set JAVA_HOME=c:\jdk-11.0.7
set JAVA=%JAVA_HOME%\bin\java
set JCROOT=c:\data\workspace\jconductor
%JAVA% -cp  %JCROOT%/out/production/jconductor;%JCROOT%/lib/*  -Dorg.slf4j.simpleLogger.showDateTime=true -Dorg.slf4j.simpleLogger.dateTimeFormat="[yyyy/MM/dd H:mm:ss.SSS]" com.netbric.s5.conductor.Main -c c:/data/workspace/jconductor/s5-win.conf
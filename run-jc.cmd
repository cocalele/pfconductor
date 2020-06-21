set JAVA_HOME=c:\jdk-11.0.7
set JAVA=%JAVA_HOME%\bin\java
set JCROOT=c:\data\workspace\jconductor
%JAVA% -cp  %JCROOT%/out/production/jconductor;%JCROOT%/lib/* com.netbric.s5.conductor.Main -c c:/data/workspace/jconductor/s5-win.conf
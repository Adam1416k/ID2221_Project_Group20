@echo off
echo Setting up Hadoop for Windows...

REM Create Hadoop directory
if not exist "C:\hadoop" mkdir C:\hadoop
cd C:\hadoop

echo Downloading Hadoop 3.3.4...
REM You'll need to download this manually from Apache Hadoop website
echo Please download hadoop-3.3.4.tar.gz from:
echo https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
echo.
echo Extract it to C:\hadoop\hadoop-3.3.4\
echo.

echo Setting up Windows binaries...
REM Create bin directory for winutils
if not exist "C:\hadoop\hadoop-3.3.4\bin" mkdir C:\hadoop\hadoop-3.3.4\bin

echo Download winutils.exe and hadoop.dll from:
echo https://github.com/steveloughran/winutils/tree/master/hadoop-3.3.4/bin
echo.
echo Copy both files to C:\hadoop\hadoop-3.3.4\bin\
echo.

echo Setting environment variables...
setx HADOOP_HOME "C:\hadoop\hadoop-3.3.4"
setx HADOOP_CONF_DIR "C:\hadoop\hadoop-3.3.4\etc\hadoop"
setx JAVA_HOME "C:\Program Files\Java\jdk-11.0.16"

REM Update PATH
for /f "tokens=2*" %%i in ('reg query "HKCU\Environment" /v PATH') do set currentpath=%%j
setx PATH "%currentpath%;C:\hadoop\hadoop-3.3.4\bin;C:\hadoop\hadoop-3.3.4\sbin"

echo.
echo Manual steps required:
echo 1. Download and extract Hadoop 3.3.4
echo 2. Download winutils.exe and hadoop.dll
echo 3. Configure Hadoop (we'll help with this next)
echo 4. Restart your command prompt
echo.
pause
@echo off
:: Set JAVA_HOME
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.27.6-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"

:: Move to project directory
cd /d "C:\Users\ishan\Downloads\projecty"

:: Run the PySpark script
echo Running Delta Lake pipeline with correct JAVA_HOME...
python generate_and_append.py

pause

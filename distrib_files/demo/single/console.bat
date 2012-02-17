@echo off
echo Console Command: console [network password]

java -cp ..\..\lib\Mckoi*.jar com.mckoi.runtime.AdminConsole -netpassword %1
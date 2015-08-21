Title: Arbitrary SQL Monitor for AppDynamics 
Author: Aaron Jacobs aaron.jacobs@appdynamics.com Date: 16 April 2015
Acknowlegements: Todd Radel, Steve Waterworth, Abhishek Pandy for their work on the original extensions.

Arbitrary SQL Monitor for AppDynamics

An AppDynamics Machine Agent extension to run SQL statements against a JDBC database, and import the results of the query as 
custom metrics in AppDynamics.

Sample use cases:
Import business metrics and KPI's from an application database.
Import performance data from SolarWinds or other system monitors that use an RDBMS backend.
This extension requires the Java Machine Agent.

Installation
Download ArbitrarySQLMonitor.zip from the Community site.
Copy ArbitrarySQLMonitor.zip into the directory where you installed the machine agent, under $AGENT_HOME/monitors.
Unzip the file. This will create a new directory called ArbitrarySQLMonitor.
In $AGENT_HOME/monitors/ArbitrarySQLMonitor, edit the file monitor.xml and configure the plugin.
Copy your JDBC driver jarfile into $AGENT_HOME/monitors/ArbitrarySQLMonitor/lib.  The extension is pre-configured with an MS SQL driver.
Restart the machine agent.

Configuration
1. Configure the SQL server instances by editing the config.yaml file in `<MACHINE_AGENT_HOME>/monitors/SQLMonitor/`. Below 
is the format
server - The SQL server name
driver - The class name of the JDBC driver to use e.g. com.mysql.jdbc.Driver
connectionString - The connection URL for the driver e.g. jdbc:mysql://localhost:3306/demo
user - The user name to connect as
password - The password for the user

2. monitor.xml properties to configure:
execution-style                   Always set to periodic with this extension
execution-frequency-in-seconds    Time period in which the extension executes

<task-arguments>
    <argument name="config-file" is-required="true" default-value="<RELATIVE_PATH>\config.yml"/>
		<argument name="machineAgent-relativePath" is-required="false" default-value="<PATH_TO timeStamp.txt>"/>
		<argument name="timeper_in_sec" is-required="false" default-value="<VALUE_PASSED_INTO_QUERIES>"/> #no effect if freqInSec used
		<argument name="execution_freq_in_secs" is-required="false" 	   default-value="<SET_TO_SAME_AS_execution-frequency-in-seconds"/>
</task-arguments>

Configure the path to the config.yaml file by editing the <task-arguments> in the monitor.xml file. Below is the sample   
<task-arguments>     <!-- config file-->      <argument name="config-file" is-required="true" default-value="monitors/SQLMonitor/config.yml"    />     ... </task-arguments>  

Note: Ensure that timeStamp.txt exists with a timeStamp.  The exact time and date should be in the past, but the value does not matter as long as it is in the past because the extension will update it to the current execution time the first time the extension is run.

3. Configure queries in config.yml with commands and displayPrefix to determine metric path in AppD metric browser.

Requirement - no duplicate data. You can ignore this section if you choose not to use the freqInSec variable in your queries. 
Set the below argument name for "machineAgent-relativePath" to the relative path of the MachineAgent folder location. For example:
<argument name="machineAgent-relativePath" is-required="false" default-value="c:\MA5\MachineAgent\monitors\ArbitrarySqlMonitor\"/>

Set the time in timeStamp.txt located in the ArbitrarySqlMonitor folder to the most current date/time in this format:
2015-07-06T20:20:10.777-05:00
Be specific to the minute when re-configuring.  This file is written to with the date/time of execution of the queries each time 
they run.  If duplicate data is detected, this date/time in the file and the current date/time are subtracted from each other and the difference is passed into the queries where freqInSec is specified in the queries for a single cycle.

Go to monitor.xml to the 'task-arguments' tag and find this tag within it:
argument name="execution_freq_in_secs"
*You must set this field equal to whatever value is configured for the 'execution-frequency-in-seconds' field within the same file. Next set argument name="timeper_in_sec" This is the value, in seconds, that will replace the variable 
freqInSec within the SQL queries in config.yml.

*Note: ALWAYS set "timeper_in_sec" LESS THAN  "execution_freq_in_secs"
This ensures no duplicate data.

For example, you would put "freqInSec" in each SQL query to replace the time, in seconds, that you wish to run the query for:
DATEADD(ss, -freqInSec, GETDATE()))

Note: If freqInSec is left out of the SQL queries in config.yml, the extension will run as designed and simply not modify any 
queries nor validate for duplicate data.

Configuration for this monitor is in the monitor.xml and yml.config files in the monitor directory.
Here is the logic used for the requirement:

1. Within each query, where you see the past data being pulled with this query snippet, for example -
X > DATEADD(ss, -1000000, GETDATE())

You will now specify the variable 'freqInSec' like this:
> DATEADD(ss, -freqInSec, GETDATE())

That variable, in seconds, is passed into the queries.  If the variable is not present, then the extension functions as designed 
without validating for duplicate data.

The extension pulls a time/date stamp from a text file in the monitor folder.  Upon first execution of the extension, the current 
time/date should be configured.

Each time the extension executes, it takes the difference between the current time/date and what is in the file.  If the 
difference is greater than the extension execution frequency (a condition that will cause duplicate data), then it takes the 
difference between the current time/date and the last execution time in the file, then passes that time (in seconds) back into 
the queries where the variable 'freqInSec' exists.

Note: To avoid duplicate data, always specify execution frequency to be Greater Than timeper_in_sec by several seconds.  Why?  
-Because each time the extension executes, we are pulling query data back for a specified time period (timeper_in_sec).  Pulling 
the query data farther back than the execution frequency will create duplicate data.

Dots below are execution frequency.
 <------|  represents the length of time we are looking back when the query is executed, for instance, with the DateAdd() function:
 [REST_OF_QUERY] > DATEADD(ss, -59, GETDATE())
 
 This query is pulling data for the last 59 seconds from the present time.  Say that the execution frequency specified in 
 monitor.xml is 60 seconds.  Then each time the extension executes, which is every 60 seconds, we get a data set covering the
 last 59 seconds.  That leaves a 1 second gap in data.  To ensure that these gaps are very small, set the execution frequency
 10 seconds or more than timeper_in_sec, which is the variable that allows values passed into the queries.  So, for example, if 
 you set the execution frequency to 240 seconds, set the timeper_in_sec variable in the range of 230 - 240.  If there is overlap in data, the extension will detect this and pass in a time that is the difference between the current time and the time the extension was last successfully executed(taken from timeStamp.txt) to the queries with the freqInSec variable.

No duplicate data, exec freq > timeper_in_sec 
.          .         .          
|----------|----------|
    <------|   <------|

Duplicate data, exec freq < timeper_in_sec 
.          .         .          
|----------|----------|
-------<--------------|


Note-Please make sure to not use tab (\t) while editing yaml files. You may want to validate the yaml file using a 
yaml validator http://yamllint.com/

JDBC Driver 
To use this extension, you will need to provide the JDBC driver, class name, and connection URL. We've provided examples for some of the common databases. You'll need to replace the placeholders (HOST, PORT, DB, etc.) in the URL with your own values.

Metrics Provided
The metrics created by this extension depend on the query you provide. The column names will be used as the metric names, and the first column of each row will be used as a folder name.
For example, the query SELECT "A", 3 as "B" would create a new metric folder called A, with a new metric B whose value would be 3.

Restrictions
The first column is assumed to be a string. All other columns are assumed to be long integers.  If you set an execution frequency longer than 1 minute, then you will get metric data gaps that will skew baselines.  The machine agent, by design, places 0 values in the spots where there are gaps in metric data.  For example, if you set the execution frequency in monitor.xml to 180 seconds, you will have two data points missing in between each metric.

Version: 1.1
Controller Compatibility: 3.6 or later
Last Updated: 21-August-2015

package com.singularity.ee.agent.systemagent.monitors;

import com.appdynamics.extensions.PathResolver;
import com.appdynamics.extensions.yml.YmlReader;
import com.google.common.base.Strings;
import com.singularity.ee.agent.systemagent.api.AManagedMonitor;
import com.singularity.ee.agent.systemagent.api.MetricWriter;
import com.singularity.ee.agent.systemagent.api.TaskExecutionContext;
import com.singularity.ee.agent.systemagent.api.TaskOutput;
import com.singularity.ee.agent.systemagent.api.exception.TaskExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ArbitrarySqlMonitor extends AManagedMonitor
{
    public String metricPrefix;
    public static final String CONFIG_ARG = "config-file";
    public static final String LOG_PREFIX = "log-prefix";
    private static String logPrefix;
    private static final Log logger = LogFactory.getLog(ArbitrarySqlMonitor.class);
    private boolean cleanFieldNames;
    private String metricPath;  
    private String dateStampFromFile = null; 
    private String relativePath = null;
    private String timeper_in_sec = null;
    private String execution_freq_in_secs = null;
    
    float diffInMillis = -1.0F;
    float diffInSec = diffInMillis / 1000;
	float diffInMin = diffInSec / 60;
    
    boolean hasDateStamp = false;
    boolean metricOverlap = false;
    BufferedReader br = null;
    Float DiffInSec = null;
    
//    //caching 
//    Cache<String, BigInteger> previousMetricsMap;
//  //  Cache<String, String> metricMap;
//	private Cache<String, Integer> metricMap;
	
	private Cache<String, ArrayList<SQLMetric>> metricMap;
	private boolean isCached = false;

	public ArbitrarySqlMonitor() 
	{
       
		 metricMap = CacheBuilder.newBuilder().expireAfterWrite(4, TimeUnit.MINUTES).build();
    }

    private String cleanFieldName(String name)
    {
    	/**
         * Unicode characters sometimes look weird in the UI, so we replace all Unicode hyphens with
         * regular hyphens. The \d{Pd} character class matches all hyphen characters.
         * @see <a href="URL#http://www.regular-expressions.info/unicode.html">this reference</a>
         */
        if(cleanFieldNames)
        {
            return name.replaceAll("\\p{Pd}", "-").replaceAll("_", " ");
        }
        else
        {
            return name;
        }
    }
    
    public TaskOutput execute(Map<String, String> taskArguments, TaskExecutionContext taskContext) throws TaskExecutionException 
    {	
    	DateTime currentTime = null;
    	DateTime timeLastExecuted = null;
    	
    	Long timeper_in_secConv = null;
    	Long execution_freq_in_secsConv = null;
    	
    	//relativePath for reading/writing time stamp that tracks last execution of queries
    	relativePath = taskArguments.get("machineAgent-relativePath");
    	relativePath += "timeStamp.txt";
    	File file = new File(relativePath);
    	FileWriter fw;
    
    	timeper_in_sec = taskArguments.get("timeper_in_sec");
    	execution_freq_in_secs = taskArguments.get("execution_freq_in_secs");
    	timeper_in_secConv = Long.valueOf(timeper_in_sec).longValue();
    	execution_freq_in_secsConv = Long.valueOf(execution_freq_in_secs).longValue();
    	
    	logger.info("timePeriod_in_sec: " + timeper_in_sec);
    	logger.info("path: " + relativePath);
    	
    	if(isCached == false)
    	{
    		currentTime = new DateTime(new DateTime());
    		timeLastExecuted = new DateTime(new DateTime());
    	}
		
        if (taskArguments != null)
        {
            try 
            {	 	
            	String sCurrentLine;     
    	        br = new BufferedReader(new FileReader(relativePath));
    			
    	        //execution frequency should always greater than time period passed into queries to prevent duplicate data
    	        if(execution_freq_in_secsConv < timeper_in_secConv)
    	        {
    	        	logger.error("CANNOT set execution_freq_in_secs in monitor.xml to a lesser value than timeper_in_sec");
    	        	logger.error("execution_freq_in_secs: " + execution_freq_in_secs);
    	        	logger.error("timeper_in_sec: " + timeper_in_sec);
    	        }
    							
    	        if(br != null)
    	        { 			 
    	        	while ((sCurrentLine = br.readLine()) != null) 
    	        	{  		
    	        		if(isCached == false)
    	        		{ 	        		
    	        			dateStampFromFile = sCurrentLine; 					
    	        			timeLastExecuted = DateTime.parse(dateStampFromFile);
    	        		}
    	        	}   			
    	        } 		
    	    } 
            catch (IOException e) 
    	    {
            	e.printStackTrace(); 
    	    } 
            finally 
    	    {
            	try 
            	{
            		if (br != null)br.close();
            	} 
            	catch (IOException ex) 
            	{
            		ex.printStackTrace();
            	}
    	    }
        	
            setLogPrefix(taskArguments.get(LOG_PREFIX));
            logger.info(getLogPrefix() + "Starting the SQL Monitoring task.");
            
            if (logger.isDebugEnabled()) 
            {
                logger.debug(getLogPrefix() + "Task Arguments Passed:" + taskArguments);
            }
            String status = "Success";
            String configFilename = getConfigFilename(taskArguments.get("config-file"));

            try 
            {
            	fw = new FileWriter(file.getAbsoluteFile());
            	BufferedWriter bw = new BufferedWriter(fw);       		       		
                Configuration config = YmlReader.readFromFile(configFilename, Configuration.class);

                if (config.getCommands().isEmpty()) 
                {
                    return new TaskOutput("Failure");
                }
               
                if(isCached == false)
                {
                	logger.info("instant time (current time): " + currentTime);
                	logger.info("old time (Time last executed query): " + timeLastExecuted);
                	diffInMillis = Math.abs(timeLastExecuted.getMillis() - currentTime.getMillis());
                	diffInSec = diffInMillis / 1000;
                	diffInMin = diffInSec / 60;
                }
               
                if(timeper_in_secConv > diffInSec)
                {
	                logger.info("execution frequency > time between query execution; no duplicate data.  Time in Minutes since last execution of queries: " + diffInMin );         	                	
	                logger.info("execution frequency in seconds: " + timeper_in_secConv);
	                logger.info("Time in sec: " + diffInSec);  
	                
	                if(isCached == false)
	                {
		                timeLastExecuted = new DateTime();              
		                bw.write(timeLastExecuted.toString());
		                bw.close();
		                logger.info("date written to file: " + timeLastExecuted.toString());     	
		                metricOverlap = false;	                
	                }
	                status = executeCommands(config, status); 
                }              
	            else if(timeper_in_secConv <= diffInSec)
	            {
	                logger.info("execution frequency < diffInSec");    	               	                             
	                logger.info("Time in sec: " + diffInSec);     
	                
	                if(isCached == false)
	                {
		                timeLastExecuted = new DateTime();
		                bw.write(timeLastExecuted.toString());
		                bw.close();
		                	
		                //store this in instance variable, then pass value into queries
		                DiffInSec = diffInSec;         	
		                logger.info("execution frequency < diffInMin; DiffInSec variable value: " + DiffInSec);
		                metricOverlap = true;
	                }
	                status = executeCommands(config, status);                	                }                                              
            	}
            catch (Exception ioe) 
            {
                logger.error("Exception", ioe);
            }
            return new TaskOutput(status);
        }
        throw new TaskExecutionException(getLogPrefix() + "SQL monitoring task completed with failures.");
    }
  
    private String executeCommands(Configuration config, String status) 
    {
        Connection conn = null;
        
        try 
        {
            for (Server server : config.getServers()) 
            {
                conn = connect(server);              

                for (Command command : config.getCommands()) 
                {
                    try 
                    {
                        int counter = 1;
                        logger.info("sql statement: " + counter++);
                        String statement = command.getCommand().trim();
                        String displayPrefix = command.getDisplayPrefix();
                                
                        if(displayPrefix == null)
                        {
                            logger.info("no displayPrefix set...");
                            command.setDisplayPrefix("Custom Metrics|default|");
                            logger.info("..." + command.getDisplayPrefix());
                        }
                        
                        if (statement != null) 
                        {                       	                           
                            logger.info("Running " + statement);                        
                            executeQuery(conn, statement, displayPrefix);
                        } 
                        else 
                        {
                            logger.error("Didn't find statement: " + counter);
                        }
                    } 
                    catch (Exception e) 
                    {
                        e.printStackTrace();
                    }
                }
                if (conn != null) 
                {
                    try 
                    {
                        conn.close();
                    } catch (SQLException e) 
                    {
                        e.printStackTrace();
                    }
                }
            }
        } 
        catch (SQLException sqle) 
        {
            logger.error("SQLException: ", sqle);
            status = "Failure";
        } 
        catch (ClassNotFoundException ce) 
        {
            logger.error("Class not found: ", ce);
            status = "Failure";
        } 
        finally 
        {
            if (conn != null) 
            {
            	try 
            	{
            		conn.close();
            	} 
            	catch (SQLException e) 
            	{
            		e.printStackTrace();
            	}
            }
        }
        return status;
    }

    private Data executeQuery(Connection conn, String query, String displayPrefix) 
    {
        Data retval = new Data();
        Statement stmt = null;
        java.sql.ResultSet rs = null;
        String newQuery = null;
        String customMetrics = "Custom Metrics|";
        displayPrefix = customMetrics + displayPrefix;
        
        try 
        {
            logger.info("dateStamp: " + dateStampFromFile);
            long rowcount = 0;
            stmt = conn.createStatement(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE, java.sql.ResultSet.CONCUR_READ_ONLY);
           
            ArrayList<SQLMetric> metricsList =  metricMap.getIfPresent(query);
            if(metricsList != null)
            {
            	logger.info("Cache hit for query: " + query);
            	for (Iterator iterator = metricsList.iterator(); iterator.hasNext();) 
            	{
            		SQLMetric sqlMetric = (SQLMetric) iterator.next();
            		writemetric(sqlMetric.getName(),sqlMetric.getValue());					
				}
            	isCached = true;
            }
            else
            {
            	logger.info("Cache miss for query: "+ query);
	            if(query.contains("freqInSec"))
	            {
	            	logger.info("query contains freqInSec - if loop hit ");
	            	
	            	if(metricOverlap == false)
	            	{
	            	    newQuery = query.replace("freqInSec", timeper_in_sec);
	                    rs = stmt.executeQuery(newQuery);              	
	                    logger.info("skippedMetricWrite == false... query with timeDate replaced: " + newQuery);
	            	}
	            	else if(metricOverlap == true)
	            	{
	            	    String DiffInSecString = DiffInSec.toString();
	            	    newQuery = query.replace("freqInSec", DiffInSecString);
	                    rs = stmt.executeQuery(newQuery);
	                    logger.info("query with timeDate replaced: " + newQuery);
	            	}           	
	            }
	            else
	            {
	            	rs = stmt.executeQuery(query);
	                logger.info("No freqInSec set in monitor.xml...");
	                logger.info("display prefix: " + displayPrefix);
	            }
	            
	            //get row and column count of result set
	            int rowCount = 0;
	            while (rs.next()) 
	            {
	                ++rowCount;              
	            }
	            logger.info("row count of resultset: " + rowCount);
	            
	            ResultSetMetaData rsmd = rs.getMetaData();
	            int columnCount = rsmd.getColumnCount();
	            logger.info("column count: " + columnCount);
	            
	            //set cursor to beginning
	            rs.beforeFirst();
            
	            //this deals with the single row/column case
	            if(columnCount == 1 && rowCount == 1)
	            {           	           	
	            	if(rs.next())
	            	{
	            	    String key = cleanFieldName(rs.getMetaData().getColumnName(1));
	            	    logger.info("display prefix: " + displayPrefix);
	            	    logger.info("query result set has single row and column");
	            	    String metricName = cleanFieldName(rs.getMetaData().getColumnName(1));              	              	
	                    String value = rs.getString(1);
	                    ResultSetMetaData metaData = rs.getMetaData();
	                    String name = metaData.getColumnLabel(1);
	                    retval.setName(name);
	                    retval.setValue(value); 
	                    
	                    String nameHolder = retval.getName();
	                    String valHolder = retval.getValue();               
	                    Data data = new Data(nameHolder, valHolder);                   	                
	                    String metricPath = displayPrefix + "|" + key ;//+ "|" + metricName;
	                    
	                    if(retval.getValue() != null)
	                    {                    	         
	                    	ArrayList<SQLMetric> al = new ArrayList();
	                    	al.add(new SQLMetric(metricPath,data.getValue()));
	                    	metricMap.put(query,al);
	                    	
	                        writemetric(metricPath,data.getValue());
	                    
	                    	logger.info("metric path: " + metricPath);
	                    	logger.info("metric value: "  + " : " + rs.getString(1));
	                    }                
	            	}
	            }
	            // multi row columns returned, execute else statement below
	            else
	            {           
	          
			        while(rs.next())
			        {	        	
			        	logger.info("while loop hit for multi row column..." + rowcount);
			            String key = cleanFieldName(rs.getString(1));
			                	
			            for (int i = 2; i <= rs.getMetaData().getColumnCount(); i++)
			            {      	            			            
			                logger.info("display prefix: " + displayPrefix);
			                logger.info("query result set has multiple rows and columns");
			            	String metricName = cleanFieldName(rs.getMetaData().getColumnName(i));	            		
			            	retval.setName(metricName);
			            	retval.setValue(rs.getString(i));       
			            	
			            	String nameHolder = retval.getName();
		                    String valHolder = retval.getValue();               
		                    Data data = new Data(nameHolder, valHolder);                   	                                  
		                    String metricPath = displayPrefix + "|" + key + "|" + metricName;
			            		
			            	if(retval.getValue() != null)
			                {        
			            		ArrayList<SQLMetric> al = new ArrayList();
		                    	al.add(new SQLMetric(metricPath,data.getValue()));
		                    	metricMap.put(query,al);
		                    	
			                    writemetric(metricPath,data.getValue());	                    
			                    
			            		logger.info("metric path: " + metricPath); 
			            		logger.info("metric value: "  + " : " + rs.getString(i));
			                }  				            	
			            }
			            rowcount += 1;   	
			        }  
			        
	            }
	            isCached = false;
            }         
        } 
        catch (SQLException sqle) 
        {
            logger.error("SQLException: ", sqle);
            logger.error("timeper_in_sec (value passed to replace freqInSec): " + timeper_in_sec);
        } 
        finally 
        {
            if (rs != null) try 
            {
                rs.close();
            } 
            catch (SQLException e) 
            {}
            if (stmt != null) 
            {
            	try 
            	{
            	    stmt.close();
            	} 
            	catch (SQLException e) {}
            }
        }
        return retval;
    }

	private void writemetric( String metricPath, String metricValue) 
	{
		String aggregationType = MetricWriter.METRIC_AGGREGATION_TYPE_OBSERVATION;
		String timeRollup = MetricWriter.METRIC_TIME_ROLLUP_TYPE_CURRENT;
		String clusterRollup = MetricWriter.METRIC_CLUSTER_ROLLUP_TYPE_COLLECTIVE;                 	
		MetricWriter writer = getMetricWriter(metricPath, aggregationType, timeRollup, clusterRollup);                    	
		writer.printMetric(metricValue);
	}

    private Connection connect(Server server) throws SQLException, ClassNotFoundException 
    {
        Connection conn = null;
        String driver = server.getDriver();
        String connectionString = server.getConnectionString();
        String user = server.getUser();
        String password = server.getPassword();

        if (driver != null && connectionString != null) 
        {
            Class.forName(driver);            
            logger.info("driver: " + driver);      
            conn = DriverManager.getConnection(connectionString, user, password);
            logger.info("Got connection " + conn);
        }
        return conn;
    }   
    
    protected String getMetricPrefix()
    {
        if (metricPath != null)
        {
            if (!metricPath.endsWith("|"))
            {
                metricPath += "|";
            }
            return metricPath;
        }
        else
        {
            return "Custom Metrics|SQLMonitor|";
        }
    }


    private String getConfigFilename(String filename) 
    {
        if (filename == null) 
        {
            return "";
        }
        //for absolute paths
        if (new File(filename).exists()) 
        {
            return filename;
        }
        //for relative paths
        File jarPath = PathResolver.resolveDirectory(AManagedMonitor.class);
        String configFileName = "";
        if (!Strings.isNullOrEmpty(filename)) 
        {
            configFileName = jarPath + File.separator + filename;
        }
        return configFileName;
    }

    private String getLogPrefix() 
    {
        return logPrefix;
    }

    private void setLogPrefix(String logPrefix) 
    {
        this.logPrefix = (logPrefix != null) ? logPrefix : "";
    }

    //below main method is for testing locally in Eclipse.
    //when extension runs entry point is not this main method but execute method
    public static void main(String[] argv) throws Exception
    {
    	Map<String, String> taskArguments = new HashMap<String, String>();
    	taskArguments.put("config-file", "c:\\MA5\\MachineAgent41\\monitors\\ArbitrarySQLMonitor\\config.yml");
    	taskArguments.put("log-prefix", "[SQLMonitorAppDExt]");
    	taskArguments.put("machineAgent-relativePath", "c:\\MA5\\MachineAgent41\\monitors\\ArbitrarySQLMonitor\\");  	
    	taskArguments.put("timeper_in_sec", "179");
    	taskArguments.put("execution_freq_in_secs", "180");
    	
    	taskArguments.put("cache-timeout", "180");
        new ArbitrarySqlMonitor().execute(taskArguments, null);
    }
}

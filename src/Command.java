package com.singularity.ee.agent.systemagent.monitors;

public class Command 
{

    private String command;
    private String displayPrefix; 
    
    private String timeStampCommand;
    private String filePath;

    public String getDisplayPrefix() {return displayPrefix;}

    public void setDisplayPrefix(String displayPrefix) {this.displayPrefix = displayPrefix;}

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) 
    {
        this.command = command;
    }
    
    public String getTimeStampCommand()
    {
    	return timeStampCommand;
    }
    
    public void setTimeStampCommand(String timeStampCommand)
    {
    	this.timeStampCommand = timeStampCommand;
    }
    
    public String getFilePath()
    {
    	return filePath;
    }
    
    public void setFilePath(String filePath)
    {
    	this.filePath = filePath;
    }

}

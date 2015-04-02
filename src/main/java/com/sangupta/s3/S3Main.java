package com.sangupta.s3;

import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;

import com.sangupta.jerry.util.AssertUtils;

/**
 * Command line interface to all tools.
 * 
 * @author sangupta
 *
 */
public class S3Main {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// read all classes that extend S3Command - so that we can load all
		// tools dynamically
		Reflections reflections = new Reflections("com.sangupta.s3");
        Set<Class<? extends S3Command>> commands = reflections.getSubTypesOf(S3Command.class);
        
        // create a list
        List allCommands = new ArrayList();
        allCommands.add(Help.class);
        if(AssertUtils.isNotEmpty(commands)) {
        	allCommands.addAll(commands);
        }
        
        // build the cli
		CliBuilder<Runnable> builder = Cli.<Runnable>builder("s3")
                .withDescription("CLI for Amazon AWS S3")
                .withDefaultCommand(Help.class)
                .withCommands(allCommands);
		
		// run on args
		Cli<Runnable> gitParser = builder.build();
        gitParser.parse(args).run();
	}

}

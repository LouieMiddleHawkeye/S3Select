package com.hawkeyeinnovations.s3select;

import picocli.CommandLine;

@CommandLine.Command(name = "S3 select", subcommands = QueryLoader.class)
public class S3Select {

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new S3Select());
        int exitCode = commandLine.execute(args);
        System.exit(exitCode);
    }
}

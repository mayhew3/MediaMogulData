package com.mayhew3.mediamogul;

import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;

public class ArgumentChecker {
  private final CommandLine commands;

  public ArgumentChecker(String... args) {
    Options options = new Options();

    Option dbOption = Option.builder("db")
        .hasArg()
        .desc("Database")
        .required(true)
        .build();
    options.addOption(dbOption);

    Option modeOption = Option.builder("mode")
        .hasArg()
        .desc("Update Mode")
        .required(false)
        .build();
    options.addOption(modeOption);

    Option templateOption = Option.builder("template")
        .hasArg()
        .desc("Full path to Template File")
        .required(false)
        .build();
    options.addOption(templateOption);

    Option blogOutputOption = Option.builder("blog")
        .hasArg()
        .desc("Full path to Blog Output File")
        .required(false)
        .build();
    options.addOption(blogOutputOption);

    CommandLineParser parser = new DefaultParser();

    try {
      this.commands = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<String> getUpdateModeIdentifier() {
    if (commands.hasOption("mode")) {
      return Optional.of(commands.getOptionValue("mode"));
    } else {
      return Optional.empty();
    }
  }

  @NotNull
  public String getDBIdentifier() {
    if (commands.hasOption("db")) {
      return commands.getOptionValue("db");
    } else {
      throw new IllegalStateException("No command line argument specified for 'db'!");
    }
  }

  public Optional<String> getTemplateFilePath() {
    if (commands.hasOption("template")) {
      return Optional.of(commands.getOptionValue("template"));
    } else {
      return Optional.empty();
    }
  }

  public Optional<String> getBlogOutputFilePath() {
    if (commands.hasOption("blog")) {
      return Optional.of(commands.getOptionValue("blog"));
    } else {
      return Optional.empty();
    }
  }
}

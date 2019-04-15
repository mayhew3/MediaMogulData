package com.mayhew3.mediamogul;

import org.apache.commons.cli.*;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ArgumentChecker {
  private CommandLine commands;
  private List<Option> optionList;
  private String[] args;
  private Boolean finalized = false;

  public ArgumentChecker(String... args) {
    this.optionList = new ArrayList<>();
    this.args = args;

    Option dbOption = Option.builder("db")
        .hasArg()
        .desc("Database")
        .required(true)
        .build();
    optionList.add(dbOption);

    Option modeOption = Option.builder("mode")
        .hasArg()
        .desc("Update Mode")
        .required(false)
        .build();
    optionList.add(modeOption);

  }

  private void maybeFinalize() {
    if (!finalized) {
      finalizeChecker();
    }
  }

  private void finalizeChecker() {
    finalized = true;

    Options options = new Options();
    for (Option option : optionList) {
      options.addOption(option);
    }

    CommandLineParser parser = new DefaultParser();

    try {
      this.commands = parser.parse(options, args);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public void addExpectedOption(String label, Boolean required, String description) {
    Option option = Option.builder(label)
        .hasArg()
        .desc(description)
        .required(required)
        .build();
    optionList.add(option);
  }

  public void removeExpectedOption(String label) {
    Optional<Option> matching = optionList.stream()
        .filter(option -> label.equalsIgnoreCase(option.getOpt()))
        .findFirst();
    if (matching.isPresent()) {
      optionList.remove(matching.get());
    } else {
      throw new IllegalArgumentException("No option with name '" + label + "'.");
    }
  }

  public String getRequiredValue(String label) {
    maybeFinalize();

    if (commands.hasOption(label)) {
      String optionValue = commands.getOptionValue(label);
      if (optionValue == null) {
        throw new IllegalArgumentException("No argument value found for required argument: " + label);
      }
      return optionValue;
    } else {
      throw new IllegalArgumentException("No argument value found for required argument: " + label);
    }
  }

  public Optional<String> getOptionalIdentifier(String label) {
    maybeFinalize();

    if (commands.hasOption(label)) {
      String optionValue = commands.getOptionValue(label);
      if (optionValue == null) {
        return Optional.empty();
      } else {
        return Optional.of(optionValue);
      }
    } else {
      return Optional.empty();
    }
  }

  public Optional<String> getUpdateModeIdentifier() {
    return getOptionalIdentifier("mode");
  }

  @NotNull
  public String getDBIdentifier() {
    return getRequiredValue("db");
  }

}

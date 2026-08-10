package com.deployagram.demo.quarkus;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public record BookSuggestion(String name, String edition, String format, List<String> authors) { }

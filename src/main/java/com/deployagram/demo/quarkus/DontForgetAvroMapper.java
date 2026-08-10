package com.deployagram.demo.quarkus;

import com.deployagram.demo.quarkus.avro.BookSuggestionMessage;
import com.deployagram.demo.quarkus.avro.DontForgetMessage;

final class DontForgetAvroMapper {

    private DontForgetAvroMapper() {
    }

    static DontForgetMessage toMessage(String email, BookSuggestion suggestion) {
        BookSuggestionMessage suggestionMessage = BookSuggestionMessage.newBuilder()
                .setName(suggestion.name())
                .setEdition(suggestion.edition())
                .setFormat(suggestion.format())
                .setAuthors(suggestion.authors())
                .build();

        return DontForgetMessage.newBuilder()
                .setEmail(email)
                .setSuggestion(suggestionMessage)
                .build();
    }
}

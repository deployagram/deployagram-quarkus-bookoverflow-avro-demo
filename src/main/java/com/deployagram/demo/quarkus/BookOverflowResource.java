package com.deployagram.demo.quarkus;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

@Path("/BookOverflow")
public class BookOverflowResource {

    @Inject
    private BookService bookService;

    @GET
    @Path("/suggestion")
    @Produces(MediaType.APPLICATION_JSON)
    public BookSuggestion suggestBook(@QueryParam("email") String email) {
        return bookService.recommend(email);
    }
}

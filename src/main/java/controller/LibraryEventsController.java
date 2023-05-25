package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
public class LibraryEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        //libraryEventProducer.sendLibraryEvent(libraryEvent);  ---> AAPROACH 1
        //SYNCHRONOUS APPROACH
        //SendResult<Integer,String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        //AAPROACH 2
        libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        if(libraryEvent.getLibraryEventId() == null)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library event id");
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}

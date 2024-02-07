package project1.sbwebapp;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

@RestController
public class SkierController {

    @PostMapping("/project1/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}")
    public ResponseEntity<String> getSkierInfo(@PathVariable("resortID") String resortID,
            @PathVariable("seasonID") String seasonID,
            @PathVariable("dayID") String dayID,
            @PathVariable("skierID") String skierID) {

        String responseMessage = String.format("Resort: %s, Season: %s, Day: %s, Skier: %s",
                resortID, seasonID, dayID, skierID);

        return new ResponseEntity<>(responseMessage, HttpStatus.CREATED);
    }
}

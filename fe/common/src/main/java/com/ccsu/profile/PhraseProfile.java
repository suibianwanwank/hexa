package com.ccsu.profile;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PhraseProfile {

    private Phrase phrase;

    private long during;

    private String printMessage;

    public PhraseProfile(Phrase phrase, long during, String printMessage) {
        this.phrase = phrase;
        this.during = during;
        this.printMessage = printMessage;
    }
}

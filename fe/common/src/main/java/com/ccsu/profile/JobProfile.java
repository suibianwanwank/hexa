package com.ccsu.profile;

import com.ccsu.utils.TableFormatUtil;
import com.ccsu.utils.TimeUtils;
import com.github.freva.asciitable.HorizontalAlign;
import com.google.common.base.Ticker;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobProfile {

    private static final String[] RETURN_COLUMNS = {"PhraseName", "DuringTime", "PhraseInfo"};

    private String sql;

    private long beginTime;

    private long endTime;

    private List<PhraseProfile> profiles;

    public JobProfile(String sql) {
        this.sql = sql;
        this.profiles = new ArrayList<>();
        this.beginTime = System.currentTimeMillis();
    }

    public void addPhraseProfile(PhraseProfile phraseProfile) {
        profiles.add(phraseProfile);
    }

    public void finish() {
        this.endTime = System.currentTimeMillis();
    }

    public String formatProfileTable() {
        String[][] data = new String[profiles.size()][RETURN_COLUMNS.length];

        for (int i = 0; i < profiles.size(); i++) {
            PhraseProfile profile = profiles.get(i);
            data[i][0] = profile.getPhrase().name();
            data[i][1] = String.valueOf(TimeUtils.formatNanosToMsString(profile.getDuring()));
            data[i][2] = profile.getPrintMessage();
        }

        String table =
                TableFormatUtil.generateCenterTable(RETURN_COLUMNS, data, HorizontalAlign.CENTER, HorizontalAlign.LEFT);

        StringBuilder builder = new StringBuilder();

        builder.append("Execute sql:\n").append(sql).append("\n\n");

        String totalTime = TimeUtils.formatMsTimeToMsString(endTime - beginTime);
        builder.append(String.format("StartTime: %s, FinishedTime: %s, TotalCost: %s \n\n", millSecondTimeToLocalDateTime(beginTime),
                millSecondTimeToLocalDateTime(endTime), totalTime));

        builder.append(table);
        return builder.toString();
    }

    public String formatExplain() {
        StringBuilder builder = new StringBuilder();

        for (PhraseProfile profile : profiles) {
            //Ignore sql parser profile
            if (profile.getPhrase() == Phrase.SQL_PARSER) {
                continue;
            }
            builder.append(String.format("%s: (%s)\n\n%s", profile.getPhrase(),
                    TimeUtils.formatNanosToMsString(profile.getDuring()), profile.getPrintMessage())).append("\n\n");
        }
        return builder.toString();
    }

    private LocalDateTime millSecondTimeToLocalDateTime(long millTimestamp) {
        Instant instant = Instant.ofEpochMilli(millTimestamp);

        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
    }

    public long getElapsedTime() {
        return endTime - beginTime;
    }
}

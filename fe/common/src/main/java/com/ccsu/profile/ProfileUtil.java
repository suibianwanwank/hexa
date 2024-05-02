package com.ccsu.profile;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

public class ProfileUtil {


    public static void addParsePhraseJobProfile(JobProfile jobProfile,
                                                 Phrase phrase, long elapsed, SqlNode sqlNode){
        jobProfile.addPhraseProfile(new PhraseProfile(phrase, elapsed, sqlNode.getKind().name()));
    }

    public static void addPlanPhraseJobProfile(JobProfile jobProfile,
                                               Phrase phrase, long elapsed, RelNode relNode){
        jobProfile.addPhraseProfile(new PhraseProfile(phrase, elapsed, RelOptUtil.toString(relNode)));
    }
}

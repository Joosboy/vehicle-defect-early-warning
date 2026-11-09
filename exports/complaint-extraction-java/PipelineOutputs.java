package com.palantir.eddie.export;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class PipelineOutputs {
    private final Dataset<Row> datasetOutput;

    private final Dataset<Row> complaintsEnriched;

    public PipelineOutputs(Dataset<Row> datasetOutput, Dataset<Row> complaintsEnriched) {
        this.datasetOutput = datasetOutput;
        this.complaintsEnriched = complaintsEnriched;
    }

    public Dataset<Row> datasetOutput() {
        return datasetOutput;
    }

    public Dataset<Row> complaintsEnriched() {
        return complaintsEnriched;
    }
}


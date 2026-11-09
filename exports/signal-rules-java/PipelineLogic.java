package com.palantir.eddie.export;

import java.lang.String;
import java.lang.UnsupportedOperationException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.ArrayJoin;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.DateFormatClass;
import org.apache.spark.sql.catalyst.expressions.EvalMode;
import org.apache.spark.sql.catalyst.expressions.Left;
import org.apache.spark.sql.catalyst.expressions.Right;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.Option;

public final class PipelineLogic {
    public static PipelineOutputs transform(
            Dataset<Row> complaintsEnriched, Dataset<Row> recallVehicles, Dataset<Row> vehicles, Dataset<Row> recalls) {
        Dataset<Row> tagRecallsAip = unsupportedTransform("tagRecallsAip");
        Dataset<Row> applyExpression2 = tagRecallsAip.withColumn(
                "recall_component", functions.col("tags").getField("ok").getField("recall_component"));
        Dataset<Row> applyExpression1 = applyExpression2.withColumn(
                "recall_failure_mode", functions.col("tags").getField("ok").getField("recall_failure_mode"));
        Dataset<Row> applyExpression =
                applyExpression1.withColumn("tag_error", functions.col("tags").getField("error"));
        Dataset<Row> drop = applyExpression.drop("tags");
        Dataset<Row> left = drop.select(
                functions.col("tag_error").alias("tag_error"),
                functions.col("recall_failure_mode").alias("recall_failure_mode"),
                functions.col("recall_component").alias("recall_component"),
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("report_received_date").alias("report_received_date"),
                functions.col("nhtsa_component").alias("nhtsa_component"),
                functions.col("summary").alias("summary"),
                functions.col("consequence").alias("consequence"),
                functions.col("remedy").alias("remedy"));
        Dataset<Row> left1 = recallVehicles.select(
                functions.col("link_id").alias("link_id"),
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("vehicle_id").alias("vehicle_id"));
        Dataset<Row> right1 = vehicles.select(
                functions.col("vehicle_id").alias("vehicle_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("model_year").alias("model_year"));
        Dataset<Row> recallScope = joinWithKeys(
                left1,
                right1,
                new ArrayList() {
                    {
                        add(new String[] {"vehicle_id", "vehicle_id"});
                    }
                },
                "left",
                new HashMap<String, String>() {
                    {
                        put("campaign_number", "fc1d82ee-33cc-4856-a9b0-7af1b5653183");
                        put("vehicle_id", "7939865f-39ca-4e22-ab1c-1970cfe2c812");
                        put("link_id", "522392b0-6385-481a-a2f6-f5f4bc62a724");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("model_year", "fd926e18-fb14-4e85-936d-bcaa954375c5");
                        put("model", "70545459-ed58-4db2-a47d-1164ae2ad11a");
                        put("vehicle_id", "d4da8e72-1e18-4a5a-a6a4-7f78caeccdf6");
                        put("make", "f42db003-3d63-4fb9-8a46-bfdf632ecd69");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"vehicle_id", "vehicle_id"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of("vehicle_id", "campaign_number", "link_id");
                    Set<String> rightColsToKeep = Set.of("make", "model", "model_year");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right1.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left1.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> project1 = recallScope.select(
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("model_year").alias("model_year"));
        Dataset<Row> aggregate = project1.groupBy("campaign_number")
                .agg(functions.collect_set(functions.col("model_year")).alias("model_years_array"));
        Dataset<Row> applyExpression5 =
                aggregate.withColumn("sorted_years", functions.sort_array(functions.col("model_years_array"), true));
        Dataset<Row> applyExpression4 = applyExpression5.withColumn(
                "distinct_sorted_years", functions.array_distinct(functions.col("sorted_years")));
        Dataset<Row> applyExpression3 = applyExpression4.withColumn(
                "model_years_covered",
                new Column(new ArrayJoin(
                        new Column(new Cast(
                                        functions.col("distinct_sorted_years").expr(),
                                        DataTypes.createArrayType(DataTypes.StringType),
                                        /*timeZoneId=*/ Option.empty(),
                                        EvalMode.TRY()))
                                .expr(),
                        functions
                                .coalesce(functions.lit(", "), functions.lit(""))
                                .expr())));
        Dataset<Row> project = applyExpression3.select(
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("model_years_covered").alias("model_years_covered"));
        Dataset<Row> right = project.select(
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("model_years_covered").alias("model_years_covered"));
        Dataset<Row> recallObjects = joinWithKeys(
                left,
                right,
                new ArrayList() {
                    {
                        add(new String[] {"campaign_number", "campaign_number"});
                    }
                },
                "left",
                new HashMap<String, String>() {
                    {
                        put("summary", "71359597-659e-43f9-aeb5-d7bc24c7d46c");
                        put("recall_component", "0f2fa165-8083-40af-a378-caa3ab3ba5dc");
                        put("consequence", "42768d0e-905e-442c-9913-dba52694665f");
                        put("nhtsa_component", "cdcc49b0-a196-4af6-a084-bf88582a70b9");
                        put("tag_error", "adef4718-e3d8-48e2-81bb-fd1091d0adb1");
                        put("campaign_number", "e60d9971-633b-4244-9d5d-718b2c2fca6b");
                        put("remedy", "2f66533a-05da-45b4-8952-b18c7dfe5a66");
                        put("recall_failure_mode", "15a98d82-55b5-4d7a-b8ab-043ff6839353");
                        put("report_received_date", "d53000a6-0014-4f63-9f74-9bda35b55d6e");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("model_years_covered", "834972e6-0389-4c99-bcb1-6d3ceb1afc92");
                        put("campaign_number", "1872e24a-445b-4e0b-9298-106b8286f9af");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"campaign_number", "campaign_number"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of(
                            "recall_failure_mode",
                            "summary",
                            "nhtsa_component",
                            "tag_error",
                            "remedy",
                            "campaign_number",
                            "consequence",
                            "report_received_date",
                            "recall_component");
                    Set<String> rightColsToKeep = Set.of("model_years_covered");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> applyExpression7 = vehicles.withColumn(
                "model_year_str",
                new Column(new Cast(
                        functions.col("model_year").expr(),
                        DataTypes.StringType,
                        /*timeZoneId=*/ Option.empty(),
                        EvalMode.TRY())));
        Dataset<Row> applyExpression6 = applyExpression7.withColumn(
                "display_name",
                functions.concat_ws(
                        " ",
                        new Column(new Cast(
                                functions.col("model_year").expr(),
                                DataTypes.StringType,
                                /*timeZoneId=*/ Option.empty(),
                                EvalMode.TRY())),
                        functions.initcap(functions.col("make")),
                        functions.initcap(functions.col("model"))));
        Dataset<Row> drop1 = applyExpression6.drop("model_year_str");
        Dataset<Row> drop5 = complaintsEnriched.drop("vin_prefix");
        Dataset<Row> left6 = drop5.select(
                functions.col("severity").alias("severity"),
                functions.col("mileage").alias("mileage"),
                functions.col("llm_error").alias("llm_error"),
                functions.col("evidence_quote").alias("evidence_quote"),
                functions.col("owner_mentions_recall").alias("owner_mentions_recall"),
                functions.col("while_driving").alias("while_driving"),
                functions.col("failure_mode").alias("failure_mode"),
                functions.col("component").alias("component"),
                functions.col("complaint_id").alias("complaint_id"),
                functions.col("vehicle_id").alias("vehicle_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("model_year").alias("model_year"),
                functions.col("date_filed").alias("date_filed"),
                functions.col("date_of_incident").alias("date_of_incident"),
                functions.col("crash").alias("crash"),
                functions.col("fire").alias("fire"),
                functions.col("injuries").alias("injuries"),
                functions.col("deaths").alias("deaths"),
                functions.col("nhtsa_components").alias("nhtsa_components"),
                functions.col("summary").alias("summary"));
        Dataset<Row> left7 = recallScope.select(
                functions.col("link_id").alias("link_id"),
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("vehicle_id").alias("vehicle_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("model_year").alias("model_year"));
        Dataset<Row> right3 = drop.select(
                functions.col("recall_failure_mode").alias("recall_failure_mode"),
                functions.col("recall_component").alias("recall_component"),
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("report_received_date").alias("report_received_date"));
        Dataset<Row> recallCoverage = joinWithKeys(
                left7,
                right3,
                new ArrayList() {
                    {
                        add(new String[] {"campaign_number", "campaign_number"});
                    }
                },
                "inner",
                new HashMap<String, String>() {
                    {
                        put("campaign_number", "72711b12-d3ed-4346-8783-ef43a2c9f3db");
                        put("model_year", "31e6361c-9160-4b17-85e3-5f4e0b2b8877");
                        put("model", "72079ac2-1767-4267-85b8-d012bb5e0098");
                        put("vehicle_id", "f6d30c68-0018-4441-96dc-fa2f0b216129");
                        put("make", "e87cee45-5aee-4c98-8825-a9390fe802bd");
                        put("link_id", "2c084102-e256-45f7-8e25-7858b424d583");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("recall_component", "ba9b781e-fef6-4e58-b75e-a90d5db0631e");
                        put("campaign_number", "be437a30-742d-470a-b7e3-5f38ea0b6222");
                        put("recall_failure_mode", "ffb09f35-8750-4639-b3e9-fd4712de4c9c");
                        put("report_received_date", "c5df1b28-0873-49ca-bd03-74394979069d");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"campaign_number", "campaign_number"});
                        }
                    });
                    Set<String> leftColsToKeep =
                            Set.of("model", "model_year", "vehicle_id", "make", "campaign_number", "link_id");
                    Set<String> rightColsToKeep =
                            Set.of("report_received_date", "recall_component", "recall_failure_mode");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right3.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left7.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> aggregate2 = recallCoverage
                .groupBy("vehicle_id", "recall_failure_mode")
                .agg(
                        functions.min(functions.col("campaign_number")).alias("covered_campaign"),
                        functions.min(functions.col("report_received_date")).alias("covered_since"));
        Dataset<Row> project3 = aggregate2.select(
                functions.col("vehicle_id").alias("vehicle_id"),
                functions.col("recall_failure_mode").alias("recall_failure_mode"),
                functions.col("covered_campaign").alias("covered_campaign"),
                functions.col("covered_since").alias("covered_since"));
        Dataset<Row> right2 = project3.select(
                functions.col("vehicle_id").alias("vehicle_id"),
                functions.col("recall_failure_mode").alias("recall_failure_mode"),
                functions.col("covered_campaign").alias("covered_campaign"),
                functions.col("covered_since").alias("covered_since"));
        Dataset<Row> complexLeftJoin4 = joinWithKeys(
                left6,
                right2,
                new ArrayList() {
                    {
                        add(new String[] {"vehicle_id", "vehicle_id"});
                        add(new String[] {"failure_mode", "recall_failure_mode"});
                    }
                },
                "left",
                new HashMap<String, String>() {
                    {
                        put("severity", "1dddae94-e52f-4f71-a18c-5333a025b1c7");
                        put("summary", "c7af4283-4853-4b60-856b-b4398c88194e");
                        put("owner_mentions_recall", "8995de86-72ff-491d-874b-6f72d5c2b09a");
                        put("nhtsa_components", "e5d7d243-d93b-4214-b8c7-e31690190721");
                        put("injuries", "3dd1974a-c122-4cb2-a125-574b4a691db4");
                        put("model_year", "b3a6d7ad-483b-4585-a550-c16f30b146ef");
                        put("crash", "211b86dd-c68c-4e90-85e2-0065ab3e7707");
                        put("llm_error", "b6b5e7a9-f063-4188-8632-2cb2db8ae628");
                        put("date_filed", "80cb7a75-e06d-494d-9971-2f3408f2a609");
                        put("complaint_id", "4e53fc64-34ef-4641-9345-72ab10c36170");
                        put("component", "c855a26a-1547-45f0-a08f-cbfbfa4b3cc6");
                        put("date_of_incident", "d689a303-1e19-4ed0-a16e-ccd0ee30fa05");
                        put("fire", "d1d23ad6-f185-4339-893d-3d1bd4ca054d");
                        put("evidence_quote", "3a91c71c-77aa-4404-9f9e-30e6a253b77a");
                        put("model", "45674801-ff7e-4809-bbda-b2b5a75488cd");
                        put("while_driving", "d723b043-5821-4bb2-808a-0030419c7672");
                        put("failure_mode", "f42fbb3b-7d1e-4075-a5c5-283f99cd2a69");
                        put("vehicle_id", "336d626b-4fdb-46ad-8586-5f17e8ac8bd5");
                        put("make", "019c37c3-32c6-429e-a78c-64cd19c86a15");
                        put("deaths", "29d766f4-4325-4de7-bb9c-5105d4b9dcb4");
                        put("mileage", "0b5d5ba2-c2e0-4e27-bec4-e0fd9712066b");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("covered_campaign", "47a2404c-e1c9-443b-af84-2a62f81926e2");
                        put("covered_since", "08c38235-d5c2-4aaf-a063-fb649b7d4f49");
                        put("recall_failure_mode", "d4dc2ae6-35f9-4621-9710-a8ac8bdc6183");
                        put("vehicle_id", "02f91811-575e-4211-b80b-ef8a3e4c416c");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"vehicle_id", "vehicle_id"});
                            add(new String[] {"failure_mode", "recall_failure_mode"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of(
                            "while_driving",
                            "severity",
                            "complaint_id",
                            "model_year",
                            "owner_mentions_recall",
                            "deaths",
                            "crash",
                            "date_of_incident",
                            "evidence_quote",
                            "component",
                            "llm_error",
                            "nhtsa_components",
                            "date_filed",
                            "mileage",
                            "summary",
                            "model",
                            "injuries",
                            "make",
                            "fire",
                            "failure_mode",
                            "vehicle_id");
                    Set<String> rightColsToKeep = Set.of("covered_since", "covered_campaign");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right2.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left6.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> left5 = complexLeftJoin4.select(
                functions.col("severity").alias("severity"),
                functions.col("mileage").alias("mileage"),
                functions.col("llm_error").alias("llm_error"),
                functions.col("evidence_quote").alias("evidence_quote"),
                functions.col("owner_mentions_recall").alias("owner_mentions_recall"),
                functions.col("while_driving").alias("while_driving"),
                functions.col("failure_mode").alias("failure_mode"),
                functions.col("component").alias("component"),
                functions.col("complaint_id").alias("complaint_id"),
                functions.col("vehicle_id").alias("vehicle_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("model_year").alias("model_year"),
                functions.col("date_filed").alias("date_filed"),
                functions.col("date_of_incident").alias("date_of_incident"),
                functions.col("crash").alias("crash"),
                functions.col("fire").alias("fire"),
                functions.col("injuries").alias("injuries"),
                functions.col("deaths").alias("deaths"),
                functions.col("nhtsa_components").alias("nhtsa_components"),
                functions.col("summary").alias("summary"),
                functions.col("covered_campaign").alias("covered_campaign"),
                functions.col("covered_since").alias("covered_since"));
        Dataset<Row> aggregate3 = recallCoverage
                .groupBy("model", "recall_failure_mode")
                .agg(
                        functions.min(functions.col("campaign_number")).alias("related_campaign"),
                        functions.min(functions.col("report_received_date")).alias("related_since"));
        Dataset<Row> project4 = aggregate3.select(
                functions.col("model").alias("model"),
                functions.col("recall_failure_mode").alias("recall_failure_mode"),
                functions.col("related_campaign").alias("related_campaign"),
                functions.col("related_since").alias("related_since"));
        Dataset<Row> right4 = project4.select(
                functions.col("model").alias("model"),
                functions.col("recall_failure_mode").alias("recall_failure_mode"),
                functions.col("related_campaign").alias("related_campaign"),
                functions.col("related_since").alias("related_since"));
        Dataset<Row> complexLeftJoin3 = joinWithKeys(
                left5,
                right4,
                new ArrayList() {
                    {
                        add(new String[] {"model", "model"});
                        add(new String[] {"failure_mode", "recall_failure_mode"});
                    }
                },
                "left",
                new HashMap<String, String>() {
                    {
                        put("severity", "5f7ba7e1-b1f6-408e-a72d-9a0e7b54fecc");
                        put("summary", "a5c3575f-b626-4429-9fde-8668af66f47e");
                        put("owner_mentions_recall", "7be49eaa-ac6f-4a99-8be8-0ea745dcfc82");
                        put("nhtsa_components", "7f2ad944-1e42-45f1-bfb0-f35dda6f9b27");
                        put("injuries", "2ee00f45-e2b6-47ac-9f5e-6df3123d18df");
                        put("covered_since", "6eebc401-675c-4d3f-a376-a2f5cdb9490a");
                        put("model_year", "86fa05ba-564a-406c-8489-7172304170b4");
                        put("crash", "da53b7c8-220c-4d93-b376-cc7b9e231264");
                        put("llm_error", "01e92c23-7190-4446-998a-b0b0b6119e31");
                        put("date_filed", "a3dfa2f9-890d-40c5-adc7-a37a86b7d7e3");
                        put("complaint_id", "cac8b910-a24b-44f4-b07c-30688815df89");
                        put("component", "6752a870-aff3-4b8f-bb31-a72daca0330c");
                        put("date_of_incident", "b38c3399-81ee-47b8-b4fa-62653fe692aa");
                        put("covered_campaign", "2836f9c0-ea92-4704-8f30-f53938c42da6");
                        put("fire", "6215087b-03e8-4df1-8916-ca6b6f020535");
                        put("evidence_quote", "ca526fdd-143f-45e1-935e-aaee4ef60e38");
                        put("model", "b3e22c89-5c52-41b4-b7bd-6cf926e79d55");
                        put("while_driving", "14083832-03d2-4c0e-99c5-0ea238b8c102");
                        put("failure_mode", "f3808099-dfe3-4b81-9e39-88fab928ae0b");
                        put("vehicle_id", "edd112de-aec0-47aa-8ced-78c889eef573");
                        put("make", "d2092ec8-db80-4bc3-aab6-0f79dc892986");
                        put("deaths", "8f64af24-faef-457c-8be8-beee85b45f29");
                        put("mileage", "d1359ab2-33eb-46e9-9fd6-ab0ab5e179ee");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("related_campaign", "abdc946d-50dc-4070-a1fa-26403fff2e6d");
                        put("model", "24fab360-70a9-44e0-afec-dfbbbfc967b1");
                        put("recall_failure_mode", "c9c2bd84-1875-4280-b99c-dea50fc4e4ec");
                        put("related_since", "822d0112-56df-4b62-a410-2930f0e12bd2");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"model", "model"});
                            add(new String[] {"failure_mode", "recall_failure_mode"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of(
                            "model",
                            "injuries",
                            "crash",
                            "covered_since",
                            "fire",
                            "evidence_quote",
                            "failure_mode",
                            "nhtsa_components",
                            "summary",
                            "vehicle_id",
                            "llm_error",
                            "mileage",
                            "date_of_incident",
                            "complaint_id",
                            "model_year",
                            "make",
                            "severity",
                            "component",
                            "owner_mentions_recall",
                            "date_filed",
                            "while_driving",
                            "deaths",
                            "covered_campaign");
                    Set<String> rightColsToKeep = Set.of("related_since", "related_campaign");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right4.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left5.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> applyExpression29 = complexLeftJoin3.withColumn(
                "recall_status",
                functions
                        .when(functions.col("covered_campaign").isNotNull(), functions.lit("Covered"))
                        .when(functions.col("related_campaign").isNotNull(), functions.lit("Possible scope gap"))
                        .otherwise(functions.lit("Not covered")));
        Dataset<Row> applyExpression28 = applyExpression29.withColumn(
                "matched_campaign",
                functions.coalesce(functions.col("covered_campaign"), functions.col("related_campaign")));
        Dataset<Row> applyExpression27 = applyExpression28.withColumn(
                "signal_id",
                functions
                        .when(
                                functions.coalesce(
                                        functions.col("recall_status").eqNullSafe(functions.lit("Covered")),
                                        functions.lit(false)),
                                new Column(new Cast(
                                        functions.lit(null).expr(),
                                        DataTypes.StringType,
                                        /*timeZoneId=*/ Option.empty(),
                                        EvalMode.TRY())))
                        .when(
                                functions.coalesce(
                                        functions.col("failure_mode").eqNullSafe(functions.lit("other")),
                                        functions.lit(false)),
                                new Column(new Cast(
                                        functions.lit(null).expr(),
                                        DataTypes.StringType,
                                        /*timeZoneId=*/ Option.empty(),
                                        EvalMode.TRY())))
                        .when(
                                functions.coalesce(functions.col("failure_mode").isNull(), functions.lit(false)),
                                new Column(new Cast(
                                        functions.lit(null).expr(),
                                        DataTypes.StringType,
                                        /*timeZoneId=*/ Option.empty(),
                                        EvalMode.TRY())))
                        .otherwise(functions.concat_ws("|", functions.col("model"), functions.col("failure_mode"))));
        Dataset<Row> applyExpression26 = applyExpression27.withColumn(
                "severity_weight",
                functions
                        .when(functions.col("severity").eqNullSafe(functions.lit(4)), functions.lit(15))
                        .when(functions.col("severity").eqNullSafe(functions.lit(3)), functions.lit(5))
                        .when(functions.col("severity").eqNullSafe(functions.lit(2)), functions.lit(2))
                        .otherwise(functions.lit(1)));
        Dataset<Row> applyExpression25 = applyExpression26.withColumn(
                "spot_check",
                functions.col("complaint_id").mod(functions.lit(80)).eqNullSafe(functions.lit(0)));
        Dataset<Row> applyExpression24 = applyExpression25.withColumn(
                "review_verdict",
                new Column(new Cast(
                        functions.lit(null).expr(),
                        DataTypes.StringType,
                        /*timeZoneId=*/ Option.empty(),
                        EvalMode.TRY())));
        Dataset<Row> applyExpression23 = applyExpression24.withColumn(
                "corrected_failure_mode",
                new Column(new Cast(
                        functions.lit(null).expr(),
                        DataTypes.StringType,
                        /*timeZoneId=*/ Option.empty(),
                        EvalMode.TRY())));
        Dataset<Row> drop4 = applyExpression23.drop("covered_campaign", "related_campaign");
        Dataset<Row> filter = drop4.filter(functions.col("signal_id").isNotNull());
        Dataset<Row> applyExpression22 = filter.withColumn(
                "is_serious",
                functions
                        .when(
                                functions
                                        .when(
                                                functions
                                                        .col("severity")
                                                        .isNull()
                                                        .or(functions.lit(3).isNull()),
                                                functions.lit(3).isNull())
                                        .otherwise(functions.col("severity").geq(functions.lit(3))),
                                functions.lit(1))
                        .otherwise(functions.lit(0)));
        Dataset<Row> applyExpression21 = applyExpression22.withColumn(
                "is_crash_or_fire",
                functions
                        .when(
                                functions
                                        .coalesce(functions.col("crash"), functions.lit(false))
                                        .or(functions.coalesce(functions.col("fire"), functions.lit(false))),
                                functions.lit(1))
                        .otherwise(functions.lit(0)));
        Dataset<Row> aggregate1 = applyExpression21
                .groupBy("signal_id", "make", "model", "failure_mode", "recall_status", "matched_campaign")
                .agg(
                        functions.count(functions.col("complaint_id")).alias("complaint_count"),
                        functions.sum(functions.col("severity_weight")).alias("severity_score"),
                        functions.sum(functions.col("is_serious")).alias("serious_count"),
                        functions.sum(functions.col("is_crash_or_fire")).alias("crash_or_fire_count"),
                        functions.sum(functions.col("injuries")).alias("injury_count"),
                        functions.sum(functions.col("deaths")).alias("death_count"),
                        functions.min(functions.col("date_filed")).alias("first_seen"),
                        functions.max(functions.col("date_filed")).alias("last_seen"),
                        functions.collect_set(functions.col("model_year")).alias("model_year_set"));
        Dataset<Row> applyExpression20 = aggregate1.withColumn(
                "model_years",
                new Column(new ArrayJoin(
                        new Column(new Cast(
                                        functions
                                                .sort_array(functions.col("model_year_set"), true)
                                                .expr(),
                                        DataTypes.createArrayType(DataTypes.StringType),
                                        /*timeZoneId=*/ Option.empty(),
                                        EvalMode.TRY()))
                                .expr(),
                        functions
                                .coalesce(functions.lit(", "), functions.lit(""))
                                .expr())));
        Dataset<Row> drop3 = applyExpression20.drop("model_year_set");
        Dataset<Row> left4 = drop3.select(
                functions.col("model_years").alias("model_years"),
                functions.col("signal_id").alias("signal_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("failure_mode").alias("failure_mode"),
                functions.col("recall_status").alias("recall_status"),
                functions.col("matched_campaign").alias("matched_campaign"),
                functions.col("complaint_count").alias("complaint_count"),
                functions.col("severity_score").alias("severity_score"),
                functions.col("serious_count").alias("serious_count"),
                functions.col("crash_or_fire_count").alias("crash_or_fire_count"),
                functions.col("injury_count").alias("injury_count"),
                functions.col("death_count").alias("death_count"),
                functions.col("first_seen").alias("first_seen"),
                functions.col("last_seen").alias("last_seen"));
        Dataset<Row> filter2 = drop4.filter(functions.col("signal_id").isNotNull());
        Dataset<Row> windowedProject = unsupportedTransform("windowedProject");
        Dataset<Row> filter1 = windowedProject.filter(functions
                .when(
                        functions
                                .col("evidence_rank")
                                .isNull()
                                .or(functions.lit(8).isNull()),
                        functions.col("evidence_rank").isNull())
                .otherwise(functions.col("evidence_rank").leq(functions.lit(8))));
        Dataset<Row> applyExpression31 = filter1.withColumn(
                "line",
                functions.concat_ws(
                        "",
                        new Column(new Cast(
                                functions.col("evidence_rank").expr(),
                                DataTypes.StringType,
                                /*timeZoneId=*/ Option.empty(),
                                EvalMode.TRY())),
                        functions.lit("."),
                        new Column(new Cast(
                                functions.col("complaint_id").expr(),
                                DataTypes.StringType,
                                /*timeZoneId=*/ Option.empty(),
                                EvalMode.TRY())),
                        functions.lit("("),
                        new Column(new DateFormatClass(
                                functions.col("date_filed").expr(),
                                functions.lit("yyyy-MM-dd").expr())),
                        functions.lit(", MY "),
                        new Column(new Cast(
                                functions.col("model_year").expr(),
                                DataTypes.StringType,
                                /*timeZoneId=*/ Option.empty(),
                                EvalMode.TRY())),
                        functions.lit(", severity "),
                        new Column(new Cast(
                                functions.col("severity").expr(),
                                DataTypes.StringType,
                                /*timeZoneId=*/ Option.empty(),
                                EvalMode.TRY())),
                        functions.lit("):"),
                        functions.col("evidence_quote")));
        Dataset<Row> aggregate4 = applyExpression31
                .groupBy("signal_id")
                .agg(functions.collect_list(functions.col("line")).alias("line_list"));
        Dataset<Row> applyExpression30 = aggregate4.withColumn(
                "evidence_sample",
                new Column(new ArrayJoin(
                        functions.sort_array(functions.col("line_list"), true).expr(),
                        functions
                                .coalesce(functions.lit("\\n"), functions.lit(""))
                                .expr())));
        Dataset<Row> project5 = applyExpression30.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("evidence_sample").alias("evidence_sample"));
        Dataset<Row> right5 = project5.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("evidence_sample").alias("evidence_sample"));
        Dataset<Row> complexLeftJoin2 = joinWithKeys(
                left4,
                right5,
                new ArrayList() {
                    {
                        add(new String[] {"signal_id", "signal_id"});
                    }
                },
                "left",
                new HashMap<String, String>() {
                    {
                        put("model_years", "b7e41c02-5194-4372-8734-565aaf1c2c44");
                        put("first_seen", "82df91e6-1cf1-458f-9f37-6c24f7009365");
                        put("complaint_count", "3b30ba64-12c8-462c-96a6-781cc063dc79");
                        put("last_seen", "0ff1502e-2a7a-4257-abb8-7cb350aa52ee");
                        put("matched_campaign", "a34f3bbf-1761-4280-8eb5-5cc5387c31ed");
                        put("crash_or_fire_count", "6c04919f-f543-4ad0-aadc-c923191349de");
                        put("signal_id", "b9fab827-8662-4a4d-a94a-15a96eea5f9e");
                        put("injury_count", "2c7b52c6-6028-4169-bdf6-4e67e0e0881c");
                        put("recall_status", "9501fb87-b7d6-420b-8dd2-3b7e81a05642");
                        put("death_count", "3aa522b0-6996-43d3-beef-a19c8ef13b69");
                        put("model", "1eae6670-9c6f-46e8-a346-21396d3135b5");
                        put("serious_count", "68ac8a7a-951d-45d1-8145-54a86616c219");
                        put("make", "8ae2541a-dcfa-4e01-a62c-8d3fc85e2e6c");
                        put("failure_mode", "1331c695-d521-4581-857e-fdd427a548ca");
                        put("severity_score", "9aeb2587-c989-4995-a82c-d629aed43a90");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("signal_id", "6a176298-dac2-40f4-a504-26c6e3b0aa98");
                        put("evidence_sample", "43d3e295-d471-4b84-bf91-6da262bf7258");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"signal_id", "signal_id"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of(
                            "recall_status",
                            "make",
                            "first_seen",
                            "matched_campaign",
                            "model_years",
                            "complaint_count",
                            "crash_or_fire_count",
                            "failure_mode",
                            "severity_score",
                            "signal_id",
                            "injury_count",
                            "death_count",
                            "last_seen",
                            "model",
                            "serious_count");
                    Set<String> rightColsToKeep = Set.of("evidence_sample");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right5.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left4.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> left3 = complexLeftJoin2.select(
                functions.col("model_years").alias("model_years"),
                functions.col("signal_id").alias("signal_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("failure_mode").alias("failure_mode"),
                functions.col("recall_status").alias("recall_status"),
                functions.col("matched_campaign").alias("matched_campaign"),
                functions.col("complaint_count").alias("complaint_count"),
                functions.col("severity_score").alias("severity_score"),
                functions.col("serious_count").alias("serious_count"),
                functions.col("crash_or_fire_count").alias("crash_or_fire_count"),
                functions.col("injury_count").alias("injury_count"),
                functions.col("death_count").alias("death_count"),
                functions.col("first_seen").alias("first_seen"),
                functions.col("last_seen").alias("last_seen"),
                functions.col("evidence_sample").alias("evidence_sample"));
        Dataset<Row> right6 = recallObjects.select(
                functions.col("campaign_number").alias("campaign_number"),
                functions.col("model_years_covered").alias("model_years_covered"));
        Dataset<Row> complexLeftJoin1 = joinWithKeys(
                left3,
                right6,
                new ArrayList() {
                    {
                        add(new String[] {"matched_campaign", "campaign_number"});
                    }
                },
                "left",
                new HashMap<String, String>() {
                    {
                        put("model_years", "e02c7a51-c101-4b7e-93e5-37e060eb1c71");
                        put("first_seen", "745da748-6e14-4f9c-83f0-926be95c0101");
                        put("complaint_count", "4fe46c5d-060b-4534-b28d-cd429b08b74c");
                        put("last_seen", "4f6e8495-496b-4af4-9f32-c0d81d8f8d89");
                        put("matched_campaign", "d15b9dcd-8129-4319-b234-6212fabe3365");
                        put("crash_or_fire_count", "e7a4ed1e-2297-47d8-a884-7b32fe7dad22");
                        put("signal_id", "07749247-3e57-44ca-84f7-e41b9ca54cfe");
                        put("injury_count", "fc3cd06e-feaf-4ac6-aec7-1e82f5ab1f1b");
                        put("evidence_sample", "3d9b8e5f-0622-408c-b509-88b60e9daf7f");
                        put("recall_status", "ad3db6bf-6997-4c96-a0ce-d86444531a39");
                        put("death_count", "a9cd77ad-3aeb-4154-91da-8eb50eab4def");
                        put("model", "f7e7b88e-2bc9-4c82-b8f5-6c0062e549a6");
                        put("serious_count", "ea5ed0c0-918f-4eb8-ae11-a797bd7d698b");
                        put("make", "00c724ca-f189-4228-9e06-5b3084c68af8");
                        put("failure_mode", "6872a308-17fe-4b68-a1d9-620dd800441c");
                        put("severity_score", "81e15535-e342-4ffd-82cb-6084300b04c3");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("model_years_covered", "c4fa0ff0-7a3b-4575-91e8-b66a41c75eee");
                        put("campaign_number", "c804120e-6ab3-400e-9770-c795321f0c6b");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"matched_campaign", "campaign_number"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of(
                            "complaint_count",
                            "model_years",
                            "recall_status",
                            "last_seen",
                            "death_count",
                            "injury_count",
                            "model",
                            "first_seen",
                            "matched_campaign",
                            "crash_or_fire_count",
                            "make",
                            "severity_score",
                            "signal_id",
                            "evidence_sample",
                            "serious_count",
                            "failure_mode");
                    Set<String> rightColsToKeep = Set.of("model_years_covered");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right6.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left3.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> renameColumns = complexLeftJoin1.withColumnRenamed("model_years_covered", "matched_recall_years");
        Dataset<Row> applyExpression19 = renameColumns.withColumn(
                "status",
                functions
                        .when(
                                functions
                                        .coalesce(
                                                functions
                                                        .when(
                                                                functions
                                                                        .col("severity_score")
                                                                        .isNull()
                                                                        .or(functions
                                                                                .lit(30)
                                                                                .isNull()),
                                                                functions
                                                                        .lit(30)
                                                                        .isNull())
                                                        .otherwise(functions
                                                                .col("severity_score")
                                                                .geq(functions.lit(30))),
                                                functions.lit(false))
                                        .and(functions.coalesce(
                                                functions
                                                        .when(
                                                                functions
                                                                        .col("complaint_count")
                                                                        .isNull()
                                                                        .or(
                                                                                functions
                                                                                        .lit(5)
                                                                                        .isNull()),
                                                                functions.lit(5).isNull())
                                                        .otherwise(functions
                                                                .col("complaint_count")
                                                                .geq(functions.lit(5))),
                                                functions.lit(false))),
                                functions.lit("Needs review"))
                        .otherwise(functions.lit("Monitoring")));
        Dataset<Row> applyExpression18 = applyExpression19.withColumn(
                "signal_name",
                functions.concat_ws(
                        "",
                        functions.initcap(new Column(new Left(
                                functions.col("model").expr(), functions.lit(1).expr()))),
                        functions.lower(new Column(new Right(
                                functions.col("model").expr(),
                                functions
                                        .length(functions.col("model"))
                                        .minus(functions.lit(1))
                                        .expr()))),
                        functions.lit(" - "),
                        functions.initcap(unsupportedExpression("regexReplace"))));
        Dataset<Row> applyExpression17 =
                applyExpression18.withColumn("proposal_action", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression16 =
                applyExpression17.withColumn("proposal_summary", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression15 =
                applyExpression16.withColumn("proposal_evidence", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression14 =
                applyExpression15.withColumn("decision", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression13 =
                applyExpression14.withColumn("decision_reason", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression12 =
                applyExpression13.withColumn("decision_notes", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression11 =
                applyExpression12.withColumn("decided_by", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression10 =
                applyExpression11.withColumn("proposal_drafted_at", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression9 =
                applyExpression10.withColumn("decided_at", unsupportedExpression("createNullValue"));
        Dataset<Row> applyExpression8 =
                applyExpression9.withColumn("flagged_since", unsupportedExpression("createNullValue"));
        Dataset<Row> drop2 = applyExpression8.drop("flagged_since");
        Dataset<Row> left2 = drop2.select(
                functions.col("decided_at").alias("decided_at"),
                functions.col("proposal_drafted_at").alias("proposal_drafted_at"),
                functions.col("decided_by").alias("decided_by"),
                functions.col("decision_notes").alias("decision_notes"),
                functions.col("decision_reason").alias("decision_reason"),
                functions.col("decision").alias("decision"),
                functions.col("proposal_evidence").alias("proposal_evidence"),
                functions.col("proposal_summary").alias("proposal_summary"),
                functions.col("proposal_action").alias("proposal_action"),
                functions.col("signal_name").alias("signal_name"),
                functions.col("status").alias("status"),
                functions.col("matched_recall_years").alias("matched_recall_years"),
                functions.col("model_years").alias("model_years"),
                functions.col("signal_id").alias("signal_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("failure_mode").alias("failure_mode"),
                functions.col("recall_status").alias("recall_status"),
                functions.col("matched_campaign").alias("matched_campaign"),
                functions.col("complaint_count").alias("complaint_count"),
                functions.col("severity_score").alias("severity_score"),
                functions.col("serious_count").alias("serious_count"),
                functions.col("crash_or_fire_count").alias("crash_or_fire_count"),
                functions.col("injury_count").alias("injury_count"),
                functions.col("death_count").alias("death_count"),
                functions.col("first_seen").alias("first_seen"),
                functions.col("last_seen").alias("last_seen"),
                functions.col("evidence_sample").alias("evidence_sample"));
        Dataset<Row> filter5 = drop4.filter(functions.col("signal_id").isNotNull());
        Dataset<Row> project7 = filter5.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("complaint_id").alias("complaint_id"),
                functions.col("date_filed").alias("date_filed"));
        Dataset<Row> left8 = project7.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("complaint_id").alias("complaint_id"),
                functions.col("date_filed").alias("date_filed"));
        Dataset<Row> filter6 = drop4.filter(functions.col("signal_id").isNotNull());
        Dataset<Row> project8 = filter6.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("complaint_id").alias("b_complaint_id"),
                functions.col("date_filed").alias("b_date_filed"),
                functions.col("severity_weight").alias("b_weight"));
        Dataset<Row> right8 = project8.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("b_complaint_id").alias("b_complaint_id"),
                functions.col("b_date_filed").alias("b_date_filed"),
                functions.col("b_weight").alias("b_weight"));
        Dataset<Row> complexInnerJoin = joinWithKeys(
                left8,
                right8,
                new ArrayList() {
                    {
                        add(new String[] {"signal_id", "signal_id"});
                    }
                },
                "inner",
                new HashMap<String, String>() {
                    {
                        put("date_filed", "667351dd-4165-4a33-9f69-841712ed784e");
                        put("complaint_id", "fe239bfd-f679-4bd6-960c-fe835deae64e");
                        put("signal_id", "badd970b-12ca-4bf4-92c5-12bf0bf0a34a");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("b_weight", "af2e6f60-275d-410f-accd-947156d15102");
                        put("b_date_filed", "00391fcf-2cc3-495c-9b22-e2983766a09a");
                        put("b_complaint_id", "4285a43e-57fc-438b-80f0-d6956cb254aa");
                        put("signal_id", "0adc15a6-dec2-4e79-a84c-a6b8a2b8da3f");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"signal_id", "signal_id"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of("date_filed", "signal_id", "complaint_id");
                    Set<String> rightColsToKeep = Set.of("b_complaint_id", "b_weight", "b_date_filed");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right8.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left8.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> filter4 = complexInnerJoin.filter(functions
                .coalesce(
                        functions
                                .when(
                                        functions
                                                .col("b_date_filed")
                                                .isNull()
                                                .or(functions.col("date_filed").isNull()),
                                        functions.col("date_filed").isNotNull())
                                .otherwise(functions.col("b_date_filed").lt(functions.col("date_filed"))),
                        functions.lit(false))
                .or(functions.coalesce(
                        functions
                                .coalesce(
                                        functions.col("b_date_filed").eqNullSafe(functions.col("date_filed")),
                                        functions.lit(false))
                                .and(functions.coalesce(
                                        functions
                                                .when(
                                                        functions
                                                                .col("b_complaint_id")
                                                                .isNull()
                                                                .or(functions
                                                                        .col("complaint_id")
                                                                        .isNull()),
                                                        functions
                                                                .col("b_complaint_id")
                                                                .isNull())
                                                .otherwise(functions
                                                        .col("b_complaint_id")
                                                        .leq(functions.col("complaint_id"))),
                                        functions.lit(false))),
                        functions.lit(false))));
        Dataset<Row> aggregate6 = filter4.groupBy("signal_id", "complaint_id", "date_filed")
                .agg(
                        functions.count(functions.col("b_weight")).alias("running_count"),
                        functions.sum(functions.col("b_weight")).alias("running_score"));
        Dataset<Row> filter3 = aggregate6.filter(functions
                .coalesce(
                        functions
                                .when(
                                        functions
                                                .col("running_count")
                                                .isNull()
                                                .or(functions.lit(5).isNull()),
                                        functions.lit(5).isNull())
                                .otherwise(functions.col("running_count").geq(functions.lit(5))),
                        functions.lit(false))
                .and(functions.coalesce(
                        functions
                                .when(
                                        functions
                                                .col("running_score")
                                                .isNull()
                                                .or(functions.lit(30).isNull()),
                                        functions.lit(30).isNull())
                                .otherwise(functions.col("running_score").geq(functions.lit(30))),
                        functions.lit(false))));
        Dataset<Row> aggregate5 = filter3.groupBy("signal_id")
                .agg(functions.min(functions.col("date_filed")).alias("flagged_since"));
        Dataset<Row> project6 = aggregate5.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("flagged_since").alias("flagged_since"));
        Dataset<Row> right7 = project6.select(
                functions.col("signal_id").alias("signal_id"),
                functions.col("flagged_since").alias("flagged_since"));
        Dataset<Row> complexLeftJoin = joinWithKeys(
                left2,
                right7,
                new ArrayList() {
                    {
                        add(new String[] {"signal_id", "signal_id"});
                    }
                },
                "left",
                new HashMap<String, String>() {
                    {
                        put("decided_at", "baea4e63-8528-4265-98a3-4d2f01b3de06");
                        put("first_seen", "09f34a59-a207-46f8-bae6-fbac53aa3ea8");
                        put("complaint_count", "cb856c1b-ca3d-4d07-a4ab-602e314760ca");
                        put("last_seen", "4a31ad29-8713-48e9-9d3b-851a71d8e5cb");
                        put("matched_campaign", "8d90a43d-b3af-434a-9fbb-f459e9f5d97e");
                        put("decision_notes", "9217c841-4b2c-461f-9175-72d5c0462db6");
                        put("death_count", "73878472-706a-419f-ae13-48a7821f68bb");
                        put("proposal_summary", "75909f29-cf69-4c2e-ba03-bf2d617fd9bd");
                        put("signal_name", "da73e3ad-a772-4486-bce8-f49e2e3ff583");
                        put("model", "178767f3-9885-4e08-bda3-5f273228bc28");
                        put("serious_count", "3062c814-5cb8-4fed-a5f8-bbdb73fc376e");
                        put("proposal_action", "0a0bfbcd-8d27-4539-b00b-717d51330d91");
                        put("make", "568085c2-fc97-4b65-a5d8-9ca76714184c");
                        put("failure_mode", "6d1f5d86-ce61-44b3-a999-b55c377df50f");
                        put("model_years", "9cbf6ec9-9002-4651-926e-fb1f1c4c1ceb");
                        put("proposal_drafted_at", "9e172e04-8517-4728-ac8f-9f94f9d1b2b6");
                        put("decision", "0e03780e-0298-47a5-bc22-ca3d4aec6186");
                        put("crash_or_fire_count", "dce93aab-5ad5-4fac-8f11-f06fc42af8c5");
                        put("signal_id", "41f87448-fe95-4b18-ae43-d3f1d9c20b17");
                        put("injury_count", "1c9cc375-f35c-4c03-bf78-36207198b13c");
                        put("proposal_evidence", "145c0759-f55f-4285-a51e-38d01dd1c681");
                        put("evidence_sample", "8f19349c-7ad4-4513-b636-b5e178d027f8");
                        put("decided_by", "c0f6e235-005d-418a-b457-37c5d11e2bca");
                        put("recall_status", "3d674d90-c56e-41fe-b465-483b774f2e11");
                        put("matched_recall_years", "49bb28e1-3f09-4bc1-a0da-420a9ea73cbb");
                        put("decision_reason", "ec8cd3bb-6682-4659-84a6-b3bc9e7a84dd");
                        put("status", "4e371fcf-fcad-475b-a7ef-51d3ed02bde1");
                        put("severity_score", "17f83f6b-f0ee-4150-8dfa-a0a5584e8d43");
                    }
                },
                new HashMap<String, String>() {
                    {
                        put("signal_id", "69679d7d-dde1-4e6e-ae97-1c675e2aaed6");
                        put("flagged_since", "580a874d-e81c-46c2-b17a-5a9419aed3c9");
                    }
                },
                (leftColumnMapping, rightColumnMapping) -> {
                    Set<String> rightColumnsToDrop = rightColumnsToDrop(new ArrayList() {
                        {
                            add(new String[] {"signal_id", "signal_id"});
                        }
                    });
                    Set<String> leftColsToKeep = Set.of(
                            "serious_count",
                            "matched_recall_years",
                            "decided_at",
                            "last_seen",
                            "decision_reason",
                            "crash_or_fire_count",
                            "signal_id",
                            "decision",
                            "proposal_evidence",
                            "evidence_sample",
                            "severity_score",
                            "death_count",
                            "matched_campaign",
                            "injury_count",
                            "recall_status",
                            "proposal_summary",
                            "decided_by",
                            "complaint_count",
                            "proposal_action",
                            "status",
                            "model_years",
                            "model",
                            "decision_notes",
                            "make",
                            "failure_mode",
                            "proposal_drafted_at",
                            "first_seen",
                            "signal_name");
                    Set<String> rightColsToKeep = Set.of("flagged_since");
                    Set<String> actuallyRightColumnsToDrop = rightColumnsToDrop.stream()
                            .filter(leftColsToKeep::contains)
                            .collect(Collectors.toUnmodifiableSet());
                    Stream<Column> rightCols = Arrays.stream(right7.columns())
                            .filter(rightColsToKeep::contains)
                            .filter(col -> !actuallyRightColumnsToDrop.contains(col))
                            .map(rightCol -> functions
                                    .col(rightColumnMapping.get(rightCol))
                                    .alias(rightCol));
                    Stream<Column> leftCols = Arrays.stream(left2.columns())
                            .filter(leftColsToKeep::contains)
                            .map(leftCol -> {
                                Column renamedLeft = functions.col(leftColumnMapping.get(leftCol));
                                return renamedLeft.alias(leftCol);
                            });
                    return Stream.concat(leftCols, rightCols).toArray(Column[]::new);
                });
        Dataset<Row> project2 = complexLeftJoin.select(
                functions.col("flagged_since").alias("flagged_since"),
                functions.col("decided_at").alias("decided_at"),
                functions.col("proposal_drafted_at").alias("proposal_drafted_at"),
                functions.col("decided_by").alias("decided_by"),
                functions.col("decision_notes").alias("decision_notes"),
                functions.col("decision_reason").alias("decision_reason"),
                functions.col("decision").alias("decision"),
                functions.col("proposal_evidence").alias("proposal_evidence"),
                functions.col("proposal_summary").alias("proposal_summary"),
                functions.col("proposal_action").alias("proposal_action"),
                functions.col("signal_name").alias("signal_name"),
                functions.col("status").alias("status"),
                functions.col("matched_recall_years").alias("matched_recall_years"),
                functions.col("model_years").alias("model_years"),
                functions.col("signal_id").alias("signal_id"),
                functions.col("make").alias("make"),
                functions.col("model").alias("model"),
                functions.col("failure_mode").alias("failure_mode"),
                functions.col("recall_status").alias("recall_status"),
                functions.col("matched_campaign").alias("matched_campaign"),
                functions.col("complaint_count").alias("complaint_count"),
                functions.col("severity_score").alias("severity_score"),
                functions.col("serious_count").alias("serious_count"),
                functions.col("crash_or_fire_count").alias("crash_or_fire_count"),
                functions.col("injury_count").alias("injury_count"),
                functions.col("death_count").alias("death_count"),
                functions.col("first_seen").alias("first_seen"),
                functions.col("last_seen").alias("last_seen"),
                functions.col("evidence_sample").alias("evidence_sample"));
        Dataset<Row> recallObjects1 = recallObjects.select(
                "tag_error",
                "recall_failure_mode",
                "recall_component",
                "campaign_number",
                "report_received_date",
                "nhtsa_component",
                "summary",
                "consequence",
                "remedy",
                "model_years_covered");
        Dataset<Row> vehicleObjects = drop1.select("display_name", "vehicle_id", "make", "model", "model_year");
        Dataset<Row> signalObjects = project2.select(
                "flagged_since",
                "decided_at",
                "proposal_drafted_at",
                "decided_by",
                "decision_notes",
                "decision_reason",
                "decision",
                "proposal_evidence",
                "proposal_summary",
                "proposal_action",
                "signal_name",
                "status",
                "matched_recall_years",
                "model_years",
                "signal_id",
                "make",
                "model",
                "failure_mode",
                "recall_status",
                "matched_campaign",
                "complaint_count",
                "severity_score",
                "serious_count",
                "crash_or_fire_count",
                "injury_count",
                "death_count",
                "first_seen",
                "last_seen",
                "evidence_sample");
        Dataset<Row> complaintObjects = drop4.select(
                "corrected_failure_mode",
                "review_verdict",
                "spot_check",
                "severity_weight",
                "signal_id",
                "matched_campaign",
                "recall_status",
                "llm_error",
                "evidence_quote",
                "owner_mentions_recall",
                "mileage",
                "while_driving",
                "severity",
                "failure_mode",
                "component",
                "complaint_id",
                "vehicle_id",
                "make",
                "model",
                "model_year",
                "date_filed",
                "date_of_incident",
                "crash",
                "fire",
                "injuries",
                "deaths",
                "nhtsa_components",
                "summary",
                "covered_since",
                "related_since");
        return new PipelineOutputs(recallObjects1, vehicleObjects, signalObjects, complaintObjects);
    }

    private static Dataset<Row> join(
            Dataset<Row> left, Dataset<Row> right, Column joinCondition, String type, Column[] postJoinSelect) {
        return left.join(right, joinCondition, type).select(postJoinSelect);
    }

    private static Dataset<Row> joinWithKeys(
            Dataset<Row> left,
            Dataset<Row> right,
            List<String[]> joinKeys,
            String type,
            Map<String, String> leftMappings,
            Map<String, String> rightMappings,
            BiFunction<Map<String, String>, Map<String, String>, Column[]> postJoinSelect) {
        Dataset<Row> leftRenamed = left.select(leftMappings.entrySet().stream()
                .map(entry -> functions.col(entry.getKey()).alias(entry.getValue()))
                .toArray(Column[]::new));
        Dataset<Row> rightRenamed = right.select(rightMappings.entrySet().stream()
                .map(entry -> functions.col(entry.getKey()).alias(entry.getValue()))
                .toArray(Column[]::new));
        Column joinCondition = joinKeys.stream()
                .map(pair ->
                        functions.col(leftMappings.get(pair[0])).equalTo(functions.col(rightMappings.get(pair[1]))))
                .reduce(Column::and)
                .orElseThrow();
        return join(leftRenamed, rightRenamed, joinCondition, type, postJoinSelect.apply(leftMappings, rightMappings));
    }

    public static Set<String> rightColumnsToDrop(List<String[]> joinKeys) {
        return joinKeys.stream()
                .filter(key -> key[0].equals(key[1]))
                .map(key -> key[1])
                .collect(Collectors.toUnmodifiableSet());
    }

    /** This method is used when a function does not support code gen yet. */
    static Column unsupportedExpression(String unsupported) {
        throw new UnsupportedOperationException("Expression not supported: " + unsupported);
    }

    static Dataset<Row> unsupportedTransform(String transformName) {
        throw new UnsupportedOperationException(
                "Code export has not been implemented for this transform yet: " + transformName);
    }
}

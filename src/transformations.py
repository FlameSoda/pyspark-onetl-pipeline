from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def filter_invalid_values(df: DataFrame) -> DataFrame:
    is_merchant_valid = ~(
            F.col("nameDest").startswith("M") &
            ((F.col("oldbalanceDest") != 0) | (F.col("newbalanceDest") != 0))
    )

    is_balance_valid = (
            (F.col("oldbalanceOrg") >= 0) &
            (F.col("newbalanceOrig") >= 0) &
            (F.col("oldbalanceDest") >= 0) &
            (F.col("newbalanceDest") >= 0)
    )

    is_amount_valid = (F.col("amount") >= 0)

    are_fraud_values_valid = (
            F.col("isFraud").isin([0, 1]) &
            F.col("isFlaggedFraud").isin([0, 1])
    )

    is_balance_change_valid = ~(
            (F.col("amount") > 0) &
            (F.col("oldbalanceOrg") == F.col("newbalanceOrig"))
    )

    combined_criteria = (
        is_merchant_valid &
        is_balance_valid &
        is_amount_valid &
        are_fraud_values_valid &
        is_balance_change_valid
    )
    filtered_df = df.filter(combined_criteria)
    return filtered_df


def add_calculated_fields(df: DataFrame) -> DataFrame:
    final_df = (df.withColumn("balance_change_orig", F.round(F.col("newbalanceOrig") - F.col("oldbalanceOrg"), 2)).
                withColumn("balance_change_dest", F.round(F.col("newbalanceDest") - F.col("oldbalanceDest"), 2)).
                withColumn("transaction_hour", F.col("step") % 24).
                withColumn("transaction_day", F.floor(F.col("step") / 24)))
    return final_df


def aggregate_transactions(df: DataFrame) -> DataFrame:
    new_df = df.groupby("type").agg(F.count("*").alias("amount_count"),
                                    F.round(F.sum("amount"), 2).alias("total_amount"),
                                    F.round(F.avg("isFraud") * 100, 2).alias("fraud_percentage"),
                                    F.sum("isFraud").alias("fraud_count"))
    return new_df.orderBy("fraud_percentage", ascending=False)
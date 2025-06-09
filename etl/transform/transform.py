"""
Transform script for IDX Laporan Keuangan ETL process.

This script handles the transformation of financial data using PySpark
by reading from parquet and writing the transformed data back to parquet.
"""

import logging
import os
from pyspark.sql import SparkSession, functions as F

def create_spark_session():
    """Create and configure Spark session."""
    spark = SparkSession.builder \
        .appName("IDX Financial Data Transform") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar") \
        .config("spark.jars", "/opt/spark/jars/mongo-spark-connector_2.12-3.0.1.jar") \
        .getOrCreate()
    return spark

def calculate_sum_if_exists(*columns):
    """
    Helper function to calculate sum only when at least one value exists.
    """
    condition = F.lit(False)
    for col in columns:
        condition = condition | col.isNotNull()
    
    sum_expr = F.lit(0)
    for col in columns:
        sum_expr = sum_expr + F.coalesce(col, F.lit(0))
    
    return F.when(condition, sum_expr)

def transform_data():
    """
    Transform financial data using PySpark.
    """
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting transformation process...")
    
    spark = None
    try:
        spark = create_spark_session()
        
        logging.info("Reading data from parquet file...")
        df = spark.read.parquet("/data/data.parquet")
        
        row_count = df.count()
        if row_count == 0:
            logging.warning("Input DataFrame is empty. Skipping transformation.")
            return
        
        logging.info(f"Processing {row_count} records...")
        
        # =====================================
        # 1. BANKS: G1. Banks
        # =====================================
        logging.info("Processing banks data...")
        banks_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G1. Banks"
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            F.col("facts.DescriptionOfPresentationCurrency_CurrentYearInstant.value").alias("satuan"),
            F.col("facts.LevelOfRoundingUsedInFinancialStatements_CurrentYearInstant.value").alias("pembulatan"),
            
            calculate_sum_if_exists(
                F.col("facts.InterestIncome_CurrentYearDuration.value"),
                F.col("facts.SubtotalShariaIncome_CurrentYearDuration.value")
            ).alias("revenue"),
            
            F.col("facts.ProfitFromOperation_CurrentYearDuration.value").alias("gross_profit"),
            F.col("facts.ProfitFromOperation_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.Cash_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            calculate_sum_if_exists(
                F.col("facts.BorrowingsThirdParties_CurrentYearInstant.value"),
                F.col("facts.BorrowingsRelatedParties_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            calculate_sum_if_exists(
                F.col("facts.SubordinatedLoansThirdParties_CurrentYearInstant.value"),
                F.col("facts.SubordinatedLoansRelatedParties_CurrentYearInstant.value")
            ).alias("long_term_borrowing"),
            
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 2. FINANCING SERVICES
        # =====================================
        logging.info("Processing financing services data...")
        financing_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G2. Financing Service"
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            F.col("facts.DescriptionOfPresentationCurrency_CurrentYearInstant.value").alias("satuan"),
            F.col("facts.LevelOfRoundingUsedInFinancialStatements_CurrentYearInstant.value").alias("pembulatan"),
            
            calculate_sum_if_exists(
                F.col("facts.IncomeFromMurabahahAndIstishna_CurrentYearDuration.value"),
                F.col("facts.IncomeFromConsumerFinancing_CurrentYearDuration.value"),
                F.col("facts.IncomeFromFinanceLease_CurrentYearDuration.value"),
                F.col("facts.AdministrationIncome_CurrentYearDuration.value"),
                F.col("facts.IncomeFromProvisionsAndCommissions_CurrentYearDuration.value")
            ).alias("revenue"),
            
            calculate_sum_if_exists(
                F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value"),
                F.col("facts.DepreciationOfInvestmentPropertyLeaseAssetsPropertyAndEquipmentForeclosedAssetsAndIjarahAssets_CurrentYearDuration.value")
            ).alias("gross_profit"),
            
            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            calculate_sum_if_exists(
                F.col("facts.BorrowingsThirdParties_CurrentYearInstant.value"),
                F.col("facts.CurrentAccountsWithOtherBanksThirdParties_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            calculate_sum_if_exists(
                F.col("facts.BorrowingsRelatedParties_CurrentYearInstant.value"),
                F.col("facts.BondsPayable_CurrentYearInstant.value"),
                F.col("facts.Sukuk_CurrentYearInstant.value")
            ).alias("long_term_borrowing"),
            
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 3. INVESTMENT SERVICE
        # =====================================
        logging.info("Processing investment services data...")
        investment_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G3. Investment Service"
        ).select(
            F.col("ticker").alias("emiten"),
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            F.col("facts.DescriptionOfPresentationCurrency_CurrentYearInstant.value").alias("satuan"),
            F.col("facts.LevelOfRoundingUsedInFinancialStatements_CurrentYearInstant.value").alias("pembulatan"),

            calculate_sum_if_exists(
                F.col("facts.IncomeFromBrokerageActivity_CurrentYearDuration.value"),
                F.col("facts.IncomeFromUnderwritingActivitiesAndSellingFees_CurrentYearDuration.value"),
                F.col("facts.IncomeFromInvestmentManagementServices_CurrentYearDuration.value")
            ).alias("revenue"),

            F.when(
                (F.col("facts.IncomeFromBrokerageActivity_CurrentYearDuration.value").isNotNull() |
                F.col("facts.IncomeFromUnderwritingActivitiesAndSellingFees_CurrentYearDuration.value").isNotNull() |
                F.col("facts.IncomeFromInvestmentManagementServices_CurrentYearDuration.value").isNotNull()) &
                F.col("facts.GeneralAndAdministrativeExpenses_CurrentYearDuration.value").isNotNull(),
                F.coalesce(F.col("facts.IncomeFromBrokerageActivity_CurrentYearDuration.value"), F.lit(0)) +
                F.coalesce(F.col("facts.IncomeFromUnderwritingActivitiesAndSellingFees_CurrentYearDuration.value"), F.lit(0)) +
                F.coalesce(F.col("facts.IncomeFromInvestmentManagementServices_CurrentYearDuration.value"), F.lit(0)) -
                F.col("facts.GeneralAndAdministrativeExpenses_CurrentYearDuration.value")
            ).alias("gross_profit"),

            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),

            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            F.col("facts.BankLoans_CurrentYearInstant.value").alias("short_term_borrowing"),
            
            F.when(
                F.col("facts.BankLoans_PriorEndYearInstant.value").isNotNull() &
                F.col("facts.BankLoans_CurrentYearInstant.value").isNotNull(),
                F.col("facts.BankLoans_PriorEndYearInstant.value") - F.col("facts.BankLoans_CurrentYearInstant.value")
            ).alias("long_term_borrowing"),
            
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 4. INSURANCE
        # =====================================
        logging.info("Processing insurance data...")
        insurance_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G4. Insurance"
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            F.col("facts.DescriptionOfPresentationCurrency_CurrentYearInstant.value").alias("satuan"),
            F.col("facts.LevelOfRoundingUsedInFinancialStatements_CurrentYearInstant.value").alias("pembulatan"),
            
            F.col("facts.RevenueFromInsurancePremiums_CurrentYearDuration.value").alias("revenue"),
            F.when(
                F.col("facts.RevenueFromInsurancePremiums_CurrentYearDuration.value").isNotNull() &
                (F.col("facts.ClaimExpenses_CurrentYearDuration.value").isNotNull() |
                 F.col("facts.ReinsuranceClaims_CurrentYearDuration.value").isNotNull()),
                F.col("facts.RevenueFromInsurancePremiums_CurrentYearDuration.value") -
                F.coalesce(F.col("facts.ClaimExpenses_CurrentYearDuration.value"), F.lit(0)) -
                F.coalesce(F.col("facts.ReinsuranceClaims_CurrentYearDuration.value"), F.lit(0))
            ).alias("gross_profit"),
            
            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            calculate_sum_if_exists(
                F.col("facts.ClaimPayables_CurrentYearInstant.value"),
                F.col("facts.ReinsurancePayables_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            F.col("facts.InsuranceLiabilitiesForFuturePolicyBenefits_CurrentYearInstant.value").alias("long_term_borrowing"),
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # 5. OTHER SECTORS
        # =====================================
        logging.info("Processing general sectors data...")
        non_special_df = df.filter(
            ~F.col("facts.Subsector_CurrentYearInstant.value").isin(["G1. Banks", "G2. Financing Service", "G3. Investment Service", "G4. Insurance"])
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("facts.CurrentPeriodEndDate_CurrentYearInstant.value").alias("report_date"),
            F.col("facts.DescriptionOfPresentationCurrency_CurrentYearInstant.value").alias("satuan"),
            F.col("facts.LevelOfRoundingUsedInFinancialStatements_CurrentYearInstant.value").alias("pembulatan"),
            F.col("facts.SalesAndRevenue_CurrentYearDuration.value").alias("revenue"),
            F.col("facts.GrossProfit_CurrentYearDuration.value").alias("gross_profit"),
            F.col("facts.ProfitLossBeforeIncomeTax_CurrentYearDuration.value").alias("operating_profit"),
            F.col("facts.ProfitLoss_CurrentYearDuration.value").alias("net_profit"),
            F.col("facts.CashAndCashEquivalents_CurrentYearInstant.value").alias("cash"),
            F.col("facts.Assets_CurrentYearInstant.value").alias("total_assets"),
            
            F.coalesce(
                F.col("facts.ShortTermBankLoans_CurrentYearInstant.value"),
                F.col("facts.CurrentMaturitiesOfBankLoans_CurrentYearInstant.value"),
                F.col("facts.OtherCurrentFinancialLiabilities_CurrentYearInstant.value"),
                F.col("facts.ShortTermDerivativeFinancialLiabilities_CurrentYearInstant.value"),
                F.col("facts.CurrentAdvancesFromCustomersThirdParties_CurrentYearInstant.value")
            ).alias("short_term_borrowing"),
            
            F.col("facts.LongTermBankLoans_CurrentYearInstant.value").alias("long_term_borrowing"),
            F.col("facts.Equity_CurrentYearInstant.value").alias("total_equity"),
            F.col("facts.Liabilities_CurrentYearInstant.value").alias("liabilities"),
            F.col("facts.NetCashFlowsReceivedFromUsedInOperatingActivities_CurrentYearDuration.value").alias("cash_dari_operasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInInvestingActivities_CurrentYearDuration.value").alias("cash_dari_investasi"),
            F.col("facts.NetCashFlowsReceivedFromUsedInFinancingActivities_CurrentYearDuration.value").alias("cash_dari_pendanaan")
        )
        
        # =====================================
        # Combine all DataFrames
        # =====================================
        logging.info("Combining all sector data...")
        final_df = (
            banks_df
            .unionByName(financing_df)
            .unionByName(investment_df) 
            .unionByName(insurance_df)
            .unionByName(non_special_df)
        )
        final_df = final_df.orderBy("emiten", ascending=True)
        
        final_df.cache()
        record_count = final_df.count()

        if record_count == 0:
            logging.warning("Transformed DataFrame is empty. No data will be saved.")
            return

        logging.info(f"Transformation completed successfully with {record_count} records")
        
        # Save transformed data to parquet
        final_df.write.format("parquet") \
            .mode("overwrite") \
            .save("/data/data.parquet")
        
        logging.info("Successfully saved transformed data to /data/data.parquet")

    except Exception as e:
        logging.error(f"An error occurred during data transformation: {str(e)}", exc_info=True)
        raise
    finally:
        if spark: 
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    transform_data()

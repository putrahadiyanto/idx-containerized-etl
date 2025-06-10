"""
Transform script for IDX Laporan Keuangan ETL process.

This script handles the transformation of financial data using PySpark
by reading from parquet and writing the transformed data back to parquet.
"""

import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession, functions as F

def get_current_quarter():
    """
    Determine the current quarter based on the current month
    
    Returns:
        tuple: (year, quarter) tuple where year is a string and quarter is an integer
    """
    CURRENT_YEAR_STR = str(datetime.now().year)
    CURRENT_MONTH = datetime.now().month
    QUARTER = 0

    if CURRENT_MONTH <= 3:
        QUARTER = 4
        CURRENT_YEAR_STR = str(int(CURRENT_YEAR_STR) - 1)
    elif CURRENT_MONTH <= 6:
        QUARTER = 1
    elif CURRENT_MONTH <= 9:
        QUARTER = 2
    else:
        QUARTER = 3
        
    return (CURRENT_YEAR_STR, QUARTER)

def create_spark_session():
    """Create and configure Spark session with MongoDB connector."""
    # Get current quarter for MongoDB collection names
    CURRENT_YEAR_STR, QUARTER = get_current_quarter()
    
    # MongoDB input collection name
    input_collection = f"idx_lapkeu{CURRENT_YEAR_STR}TW{QUARTER}"
    
    spark = SparkSession.builder \
        .appName("IDX Financial Data Transform") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.maxResultSize", "256m") \
        .config("spark.driver.cores", "1") \
        .config("spark.executor.cores", "1") \
        .config("spark.default.parallelism", "2") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.mongodb.input.uri", f"mongodb://host.docker.internal:27017/idx_lapkeu.{input_collection}") \
        .master("local[1]") \
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
    Transform financial data using PySpark by reading from MongoDB.
    """
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting transformation process...")
    
    spark = None
    try:
        spark = create_spark_session()
        
        logging.info("Reading data from MongoDB...")
        df = spark.read.format("mongo").load()
        
        row_count = df.count()
        if row_count == 0:
            logging.warning("Input DataFrame from MongoDB is empty. Skipping transformation.")
            return
        
        logging.info(f"Processing {row_count} records from MongoDB...")
        
        # =====================================
        # 1. BANKS: G1. Banks
        # =====================================
        logging.info("Processing banks data...")
        banks_df = df.filter(
            F.col("facts.Subsector_CurrentYearInstant.value") == "G1. Banks"
        ).select(
            F.col("facts.EntityName_CurrentYearInstant.value").alias("entity_name"),
            F.col("ticker").alias("emiten"),
            F.col("tahun").alias("tahun"),
            F.col("triwulan").alias("triwulan"),
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
            F.col("tahun").alias("tahun"),
            F.col("triwulan").alias("triwulan"),
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
            F.col("tahun").alias("tahun"),
            F.col("triwulan").alias("triwulan"),
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
            F.col("tahun").alias("tahun"),
            F.col("triwulan").alias("triwulan"),
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
            F.col("tahun").alias("tahun"),
            F.col("triwulan").alias("triwulan"),
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
            .unionByName(non_special_df)        )
        final_df = final_df.orderBy("emiten", ascending=True)
        final_df.cache()
        record_count = final_df.count()
        
        if record_count == 0:
            logging.warning("Transformed DataFrame is empty. No data will be saved.")
            return
            
        logging.info(f"Transformation completed successfully with {record_count} records")

        
        # Get current quarter for JSON filename
        CURRENT_YEAR_STR, QUARTER = get_current_quarter()
        json_output_path = f"/data/transformed_data_{CURRENT_YEAR_STR}TW{QUARTER}"
        
        # Save transformed data to JSON file using PySpark
        logging.info(f"Saving transformed data to JSON path: {json_output_path}")
        
        # Save as JSON using PySpark (this creates a directory with part files)
        final_df.coalesce(1).write.mode("overwrite").json(json_output_path)
        
        logging.info(f"Successfully saved {record_count} records to JSON path: {json_output_path}")

    except Exception as e:
        logging.error(f"An error occurred during data transformation: {str(e)}", exc_info=True)
        raise
    finally:
        if spark: 
            spark.stop()
            logging.info("Spark session stopped.")

if __name__ == "__main__":
    transform_data()

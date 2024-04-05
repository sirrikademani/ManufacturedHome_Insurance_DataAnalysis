create schema covertree.transformations;

--transformations

-- CONVERT TEXT DATE VALUES INTO TIME STAMP IN POLICY TABLE:
-- REPLACE STRINGS '[NULL]' as null in POLICY

CREATE OR REPLACE TABLE COVERTREE.TRANSFORMATIONS.POLICY_1 AS (
SELECT
  POLICYHOLDER_LOCATOR, 
  QUOTE_LOCATOR, 
  POLICY_LOCATOR, 
  STATE, 
  --ISSUED_TIMESTAMP,
  case when ISSUED_TIMESTAMP='[NULL]' THEN null ELSE TO_TIMESTAMP(ISSUED_TIMESTAMP, 'MM/DD/YY HH24:MI') END as ISSUED_TIMESTAMP,
  ISSUED_FLAG, 
  MAIN_POLICY_LOCATOR_FLAG, 
  --CREATED_TIMESTAMP,
  case when CREATED_TIMESTAMP='[NULL]' THEN null ELSE TO_TIMESTAMP(CREATED_TIMESTAMP, 'MM/DD/YY HH24:MI') END as CREATED_TIMESTAMP,
  --POLICY_START_TIMESTAMP, 
  case when POLICY_START_TIMESTAMP='[NULL]' THEN null ELSE TO_TIMESTAMP(POLICY_START_TIMESTAMP, 'MM/DD/YY HH24:MI') END as POLICY_START_TIMESTAMP,
  --POLICY_END_TIMESTAMP, 
  case when POLICY_END_TIMESTAMP='[NULL]' THEN null ELSE TO_TIMESTAMP(POLICY_END_TIMESTAMP, 'MM/DD/YY HH24:MI') END as POLICY_END_TIMESTAMP,
  DATE_OF_BIRTH, 
  INSURANCE_SCORE, 
  AGENCY_ID, 
  AGENT_ID, 
  PRIOR_INSURANCE, 
  PRIOR_CARRIER_NAME, 
  PRIOR_POLICY_EXPIRATION_DATE, 
  APPLICATION_INTIATION, 
  POLICY_PREMIUM
FROM
  COVERTREE.STAGING.POLICY)
;

select * from  COVERTREE.STAGING.POLICY_1;

--CREATING QUOTE_ISSUED_DATE:  A DATE THAT CONTAINS ISSUED DATE OF THE POLICY VALUE OR IF THE ISSUED DATE IS NULL, THEN IT TAKES THE QUOTE CREATED DATE
-- ADDED COLUMN ISSUED_PREMIUM_AMOUNT
-- ADDED COLUMNS POLICY_TENURE_IN_YEARS
-- ADDED AGE COLUMN TO FETCH AGE OF THE POLICY HOLDER
create or replace table COVERTREE.TRANSFORMATIONS.policy_transformation as
(select POLICYHOLDER_LOCATOR, 
cast(QUOTE_LOCATOR as varchar(16777216)) as QUOTE_LOCATOR, 
POLICY_LOCATOR, 
STATE, 
ISSUED_TIMESTAMP, 
ISSUED_FLAG, 
MAIN_POLICY_LOCATOR_FLAG, 
CREATED_TIMESTAMP, 
(case when issued_timestamp is not null then issued_timestamp else created_timestamp end )as quote_issued_timestamp,
DATE(case when issued_timestamp is not null then issued_timestamp else created_timestamp end )as quote_issued_date,
POLICY_START_TIMESTAMP, 
POLICY_END_TIMESTAMP, 
ABS(DATEDIFF(MONTH, POLICY_END_TIMESTAMP, POLICY_START_TIMESTAMP) ) AS POLICY_TENURE_IN_MONTHS,
DATE_OF_BIRTH, 
DATE_PART('year', CURRENT_DATE()) - DATE_PART('year', DATE_OF_BIRTH) - 
    CASE
        WHEN DATE_PART('month', CURRENT_DATE()) < DATE_PART('month', DATE_OF_BIRTH) THEN 1
        WHEN DATE_PART('month', CURRENT_DATE()) = DATE_PART('month', DATE_OF_BIRTH) AND
             DATE_PART('day', CURRENT_DATE()) < DATE_PART('day', DATE_OF_BIRTH) THEN 1
        ELSE 0
    END AS AGE,
INSURANCE_SCORE, 
AGENCY_ID, 
AGENT_ID, 
PRIOR_INSURANCE, 
PRIOR_CARRIER_NAME, 
PRIOR_POLICY_EXPIRATION_DATE, 
APPLICATION_INTIATION, 
POLICY_PREMIUM,
(CASE WHEN STATE= 'accepted' then policy_premium else 0 end )as ISSUED_PREMIUM_AMT
from COVERTREE.TRANSFORMATIONS.policy_1);

SELECT * FROM COVERTREE.TRANSFORMATIONS.policy_transformation;

-- fetching coverage details per QUOTE_EXPOSURE_LOCATOR
CREATE OR REPLACE TABLE COVERTREE.TRANSFORMATIONS.COVERAGE_DETAILS AS(
select 
 QUOTE_EXPOSURE_LOCATOR
--, PERIL_LOCATOR 
--, NAME
--, FIELD_NAME
--, FIELD_VALUE
,MAX(case when field_name='medical_payment_to_others_limit_per_person' then field_value end) as "medical_payment_to_others_limit_per_person"
,MAX(case when field_name='identity_fraud_limit' then field_value end) as "identity_fraud_limit"
,MAX(case when field_name='cov_c_settlement_option' then field_value end) as "cov_c_settlement_option"
,MAX(case when field_name='loss_of_use_percentage' then field_value end) as "loss_of_use_percentage"
,MAX(case when field_name='animal_liability_limit' then field_value end) as "animal_liability_limit"
,MAX(case when field_name='wind_hail_deductible' then field_value end) as "wind_hail_deductible"
,MAX(case when field_name='fungi_bacteria_property_limit' then field_value end) as "fungi_bacteria_property_limit"
,MAX(case when field_name='unscheduled_personal_property_limit' then field_value end) as "unscheduled_personal_property_limit"
,MAX(case when field_name='debris_removal_limit' then field_value end) as "debris_removal_limit"
,MAX(case when field_name='equipment_breakdown_limit' then field_value end) as "equipment_breakdown_limit"
,MAX(case when field_name='water_damage_reduced_limit' then field_value end) as "water_damage_reduced_limit"
,MAX(case when field_name='all_other_perils_deductible' then field_value end) as "all_other_perils_deductible"
,MAX(case when field_name='other_structures_limit' then field_value end) as "other_structures_limit"
,MAX(case when field_name='cov_b_settlement_option' then field_value end) as "cov_b_settlement_option"
,MAX(case when field_name='manufactured_home_limit' then field_value end) as "manufactured_home_limit"
,MAX(case when field_name='inflation_guard' then field_value end) as "inflation_guard"
,MAX(case when field_name='loss_assessment_limit' then field_value end) as "loss_assessment_limit"
,MAX(case when field_name='no_of_golf_carts' then field_value end) as "no_of_golf_carts"
,MAX(case when field_name='secondary_residence_liability_group' then field_value end) as "secondary_residence_liability_group"
,MAX(case when field_name='personal_liability' then field_value end) as "personal_liability"
,MAX(case when field_name='damage_to_property_of_others' then field_value end) as "damage_to_property_of_others"
,MAX(case when field_name='earthquake_deductible' then field_value end) as "earthquake_deductible"
,MAX(case when field_name='cov_a_settlement_option' then field_value end) as "cov_a_settlement_option"
,MAX(case when field_name='occasional_vacation_rental' then field_value end) as "occasional_vacation_rental"
,MAX(case when field_name='enhanced_coverage' then field_value end) as "enhanced_coverage"
,MAX(case when field_name='trip_collision' then field_value end) as "trip_collision"
,MAX(case when field_name='mine_sub_add_living_expense_limit' then field_value end) as "mine_sub_add_living_expense_limit"
,MAX(case when field_name='residence_burglary_limit' then field_value end) as "residence_burglary_limit"
,MAX(case when field_name='scheduled_personals' then field_value end) as "scheduled_personals"
,MAX(case when field_name='water_backup_and_sump_overflow_limit' then field_value end) as "water_backup_and_sump_overflow_limit"
,MAX(case when field_name='premises_liability_limit' then field_value end) as "premises_liability_limit"
from COVERTREE.STAGING.COVERAGES 
GROUP BY
 QUOTE_EXPOSURE_LOCATOR
--, PERIL_LOCATOR 
--, NAME
--, FIELD_NAME
--, FIELD_VALUE
);

select * from COVERTREE.transformations.COVERAGE_DETAILS;

--TRANSFORMATION for EXPOSURE_CHARACTERISTIC table
--remove null columns VACANCY_REASON, FOUR_FEET_FENCE, DIVING_BOARD, VISITORS_IN_A_MONTH, SKIRTING_TYPE, STORM_MITIGATION_FORTIFIED.
-- Calculate Model age and Roof age of the property
CREATE OR REPLACE TABLE COVERTREE.TRANSFORMATIONS.EXPOSURE_CHARACTERISTIC_TRANSFORM AS(
SELECT 
QUOTE_LOCATOR, 
QUOTE_EXPOSURE_LOCATOR, 
QUOTE_EXPOSURE_CHARACTERISTICS_LOCATOR, 
COUNTRY, 
STATE, 
STREET_ADDRESS, 
ZIP_CODE, 
CITY, 
COUNTY, 
LAT, 
LONG, 
PROPERTY_WITH_FIRE_PROTECTION, 
POLICY_USAGE, 
BUSINESS_ON_PREMISES, 
FORM, 
UNIT_ID, 
COMMUNITY_POLICY_DISCOUNT, 
UNIT_LOCATION, 
PERSONALIZED_PLAN_TYPE, 
HOME_TYPE, 
ROOF_SHAPE, 
SOURCE_OF_HEAT, 
ROOF_CONDITION, 
MODEL_YEAR, 
CASE WHEN MODEL_YEAR='[NULL]' THEN NULL ELSE YEAR(CURRENT_DATE()) - (CAST(MODEL_YEAR AS INTEGER)) END AS MODEL_AGE,
RCV, 
SWIMMING_POOL, 
MORTGAGE, 
MANUFACTURER_NAME, 
TOTAL_SQUARE_FOOTAGE, 
UNREPAIRED_DAMAGES, 
TRAMPOLINE_LIABILITY, 
ROOF_YEAR_YYYY, 
CASE WHEN ROOF_YEAR_YYYY='[NULL]' THEN NULL ELSE YEAR(CURRENT_DATE()) - (CAST(ROOF_YEAR_YYYY AS INTEGER)) END AS ROOF_AGE,
SECURE_RAILS, 
ACV, 
UTILITY_SERVICES, 
THERMO_STATIC_CONTROL, 
UNIT_IS_TIED, 
HOME_FIXTURES, 
ROOF_MATERIAL, 
UNUSUAL_RISK, 
PARK_NAME, 
PURCHASE_DATE, 
BURGLAR_ALARM, 
STORM_MITIGATION_SHUTTERS, 
STORM_MITIGATION_IMPACTGLASS, 
--STORM_MITIGATION_FORTIFIED, 
WROUGHT_IRON, 
SHORT_TERM_RENTAL_SURCHARGE, 
DAYCARE_ON_PREMISES, 
BUSINESS_EMPLOYEES_ON_PREMISES, 
SOURCE_OF_HEAT_INSTALLATION, 
TYPE_OF_FUEL, 
TRAMPOLINE_SAFETY_NET, 
--VACANCY_REASON, 
--FOUR_FEET_FENCE, 
--DIVING_BOARD, 
--VISITORS_IN_A_MONTH, 
--SKIRTING_TYPE
from COVERTREE.STAGING.EXPOSURE_CHARACTERISTIC);

-- ISSUED POLICYHOLDER INFORMATION TABLE
CREATE OR REPLACE TABLE COVERTREE.TRANSFORMATIONS.ISSUED_POLICYHOLDER_INFO AS(
SELECT
P.*,
P_HOLDER.FIRST_NAME, 
P_HOLDER.LAST_NAME, 
P_HOLDER.EMAIL_ADDRESS, 
P_HOLDER.PRIMARY_CONTACT_NUMBER, 
P_HOLDER.SECONDARY_CONTACT_NUMBER, 
P_HOLDER.MAILING_STREET_ADDRESS_POLICYHOLDER, 
P_HOLDER.MAILING_LOT_UNIT_POLICYHOLDER, 
P_HOLDER.MAILING_CITY_POLICYHOLDER, 
P_HOLDER.MAILING_STATE_POLICYHOLDER, 
P_HOLDER.MAILING_ZIP_CODE_POLICYHOLDER, 
P_HOLDER.MAILING_COUNTRY_POLICYHOLDER, 
P_HOLDER.GENDER ,
FROM (SELECT
*
FROM COVERTREE.STAGING.POLICY
WHERE STATE='accepted') AS P
LEFT JOIN COVERTREE.STAGING.POLICYHOLDER P_HOLDER 
ON P_HOLDER.POLICYHOLDER_LOCATOR=P.POLICYHOLDER_LOCATOR
);

SELECT * FROM COVERTREE.TRANSFORMATIONS.ISSUED_POLICYHOLDER_INFO;

--создание схем
CREATE SCHEMA IF NOT EXISTS DS
CREATE SCHEMA IF NOT EXISTS LOGS
CREATE SCHEMA Stage 



--Удаление данных (для теста пока)
DROP TABLE IF EXISTS stage.ft_balance_f, stage.ft_posting_f, stage.md_account_d, stage.md_currency_d, stage.md_exchange_rate_d, stage.md_ledger_account_s; 

TRUNCATE TABLE ds.ft_balance_f, ds.ft_posting_f, ds.md_account_d, ds.md_currency_d, ds.md_exchange_rate_d, ds.md_ledger_account_s RESTART IDENTITY;



--Проверка дублированных данных
SELECT "DATA_ACTUAL_DATE", "CURRENCY_RK", COUNT(*)
FROM stage.md_exchange_rate_d
GROUP BY "DATA_ACTUAL_DATE", "CURRENCY_RK"



--удаление дубликатов через ROW_NUMBER
-- WITH Del_dublicate_row AS(
-- SELECT "CURRENCY_RK", "DATA_ACTUAL_DATE",
-- 	ROW_NUMBER() OVER(PARTITION BY "CURRENCY_RK", "DATA_ACTUAL_DATE" ORDER BY (SELECT 1)) row_num
-- FROM ds.md_exchange_rate_d d
-- )

-- DELETE FROM ds.md_exchange_rate_d d
-- WHERE ("CURRENCY_RK", "DATA_ACTUAL_DATE") IN (
-- 	SELECT "CURRENCY_RK", "DATA_ACTUAL_DATE" 
-- 	FROM Del_dublicate_row
-- 	WHERE row_num > 1);




--Создание таблиц в схеме ds
CREATE TABLE IF NOT EXISTS ds.ft_balance_f
(
    "ON_DATE" date,
    "ACCOUNT_RK" bigint,
    "CURRENCY_RK" bigint,
    "BALANCE_OUT" double precision
);




CREATE TABLE IF NOT EXISTS ds.ft_posting_f
(
    "OPER_DATE" date,
    "CREDIT_ACCOUNT_RK" bigint,
    "DEBET_ACCOUNT_RK" bigint,
    "CREDIT_AMOUNT" double precision,
    "DEBET_AMOUNT" double precision
);



CREATE TABLE IF NOT EXISTS ds.md_account_d
(
    "DATA_ACTUAL_DATE" date,
    "DATA_ACTUAL_END_DATE" date,
    "ACCOUNT_RK" bigint,
    "ACCOUNT_NUMBER" text,
    "CHAR_TYPE" text,
    "CURRENCY_RK" bigint,
    "CURRENCY_CODE" bigint
);




CREATE TABLE IF NOT EXISTS ds.md_currency_d
(
    "CURRENCY_RK" bigint,
    "DATA_ACTUAL_DATE" date,
    "DATA_ACTUAL_END_DATE" date,
    "CURRENCY_CODE" double precision,
    "CODE_ISO_CHAR" text
);




CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d
(
    "DATA_ACTUAL_DATE" date,
    "DATA_ACTUAL_END_DATE" date,
    "CURRENCY_RK" bigint,
    "REDUCED_COURCE" double precision,
    "CODE_ISO_NUM" bigint
);



CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s
(
    "CHAPTER" text,
    "CHAPTER_NAME" text,
    "SECTION_NUMBER" bigint,
    "SECTION_NAME" text,
    "SUBSECTION_NAME" text,
    "LEDGER1_ACCOUNT" bigint,
    "LEDGER1_ACCOUNT_NAME" text,
    "LEDGER_ACCOUNT" bigint,
    "LEDGER_ACCOUNT_NAME" text,
    "CHARACTERISTIC" text,
    "START_DATE" date,
    "END_DATE" date
);

--Ключи первичные в слое ds
ALTER TABLE ds.ft_balance_f ADD PRIMARY KEY ("ON_DATE", "ACCOUNT_RK");
ALTER TABLE ds.md_account_d ADD PRIMARY KEY ("DATA_ACTUAL_DATE", "ACCOUNT_RK");
ALTER TABLE ds.md_currency_d ADD PRIMARY KEY ("CURRENCY_RK", "DATA_ACTUAL_DATE");
ALTER TABLE ds.md_ledger_account_s ADD PRIMARY KEY ("LEDGER_ACCOUNT", "START_DATE");
ALTER TABLE ds.md_exchange_rate_d ADD PRIMARY KEY ("DATA_ACTUAL_DATE", "CURRENCY_RK");






--Загрузка в схему ds

--ds.ft_balance_f
INSERT INTO ds.ft_balance_f AS dsb( "ON_DATE",
    "ACCOUNT_RK",
    "CURRENCY_RK",
    "BALANCE_OUT")
SELECT st."ON_DATE"::date,  -- Explicitly cast to date
       st."ACCOUNT_RK",
       st."CURRENCY_RK",
       st."BALANCE_OUT"
FROM stage.ft_balance_f AS st
ON CONFLICT ("ON_DATE", "ACCOUNT_RK") DO UPDATE
SET "CURRENCY_RK" = EXCLUDED."CURRENCY_RK",
    "BALANCE_OUT" = EXCLUDED."BALANCE_OUT"
WHERE COALESCE(dsb."CURRENCY_RK", 0) != COALESCE(EXCLUDED."CURRENCY_RK", 0) OR
      COALESCE(dsb."BALANCE_OUT", 0) != COALESCE(EXCLUDED."BALANCE_OUT", 0);



--ds.ft_posting_f
TRUNCATE TABLE ds.ft_posting_f;
INSERT INTO ds.ft_posting_f AS dsp("OPER_DATE", "CREDIT_ACCOUNT_RK", "DEBET_ACCOUNT_RK", "CREDIT_AMOUNT", "DEBET_AMOUNT")
SELECT st."OPER_DATE"::date, st."CREDIT_ACCOUNT_RK", st."DEBET_ACCOUNT_RK", st."CREDIT_AMOUNT", st."DEBET_AMOUNT"
FROM stage.ft_posting_f AS st;



-- ds.md_account_d
INSERT INTO ds.md_account_d AS dsa ("DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE", "ACCOUNT_RK", "ACCOUNT_NUMBER", "CHAR_TYPE", "CURRENCY_RK", "CURRENCY_CODE")
SELECT st."DATA_ACTUAL_DATE"::date, st."DATA_ACTUAL_END_DATE"::date, st."ACCOUNT_RK", st."ACCOUNT_NUMBER", st."CHAR_TYPE", st."CURRENCY_RK", st."CURRENCY_CODE"
FROM stage.md_account_d AS st
ON CONFLICT ("DATA_ACTUAL_DATE", "ACCOUNT_RK") DO UPDATE
SET "DATA_ACTUAL_END_DATE" = EXCLUDED."DATA_ACTUAL_END_DATE",
    "ACCOUNT_NUMBER" = EXCLUDED."ACCOUNT_NUMBER",
    "CHAR_TYPE" = EXCLUDED."CHAR_TYPE",
    "CURRENCY_RK" = EXCLUDED."CURRENCY_RK",
    "CURRENCY_CODE" = EXCLUDED."CURRENCY_CODE"
WHERE dsa."DATA_ACTUAL_END_DATE" != EXCLUDED."DATA_ACTUAL_END_DATE" OR
      COALESCE(dsa."ACCOUNT_NUMBER", '') != COALESCE(EXCLUDED."ACCOUNT_NUMBER", '') OR
      COALESCE(dsa."CHAR_TYPE", '') != COALESCE(EXCLUDED."CHAR_TYPE", '') OR
      COALESCE(dsa."CURRENCY_RK", 0) != COALESCE(EXCLUDED."CURRENCY_RK", 0) OR
      COALESCE(dsa."CURRENCY_CODE", 0) != COALESCE(EXCLUDED."CURRENCY_CODE", 0);



-- ds.md_currency_d
INSERT INTO ds.md_currency_d AS dsc ("CURRENCY_RK", "DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE", "CURRENCY_CODE", "CODE_ISO_CHAR")
SELECT st."CURRENCY_RK", st."DATA_ACTUAL_DATE"::date, st."DATA_ACTUAL_END_DATE"::date, st."CURRENCY_CODE", st."CODE_ISO_CHAR"
FROM stage.md_currency_d AS st
ON CONFLICT ("CURRENCY_RK", "DATA_ACTUAL_DATE") DO UPDATE
SET "DATA_ACTUAL_END_DATE" = EXCLUDED."DATA_ACTUAL_END_DATE",
    "CURRENCY_CODE" = EXCLUDED."CURRENCY_CODE",
    "CODE_ISO_CHAR" = EXCLUDED."CODE_ISO_CHAR"
WHERE dsc."DATA_ACTUAL_END_DATE" != EXCLUDED."DATA_ACTUAL_END_DATE" OR
      COALESCE(dsc."CURRENCY_CODE", 0) != COALESCE(EXCLUDED."CURRENCY_CODE", 0) OR
      COALESCE(dsc."CODE_ISO_CHAR", '') != COALESCE(EXCLUDED."CODE_ISO_CHAR", '');

-- ds.md_exchange_rate_d
INSERT INTO ds.md_exchange_rate_d AS dse ("DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE", "CURRENCY_RK", "REDUCED_COURCE", "CODE_ISO_NUM")
SELECT st."DATA_ACTUAL_DATE"::date, st."DATA_ACTUAL_END_DATE"::date, st."CURRENCY_RK", st."REDUCED_COURCE", st."CODE_ISO_NUM"
FROM stage.md_exchange_rate_d AS st
ON CONFLICT ("DATA_ACTUAL_DATE", "CURRENCY_RK") DO UPDATE
SET "DATA_ACTUAL_END_DATE" = EXCLUDED."DATA_ACTUAL_END_DATE",
    "REDUCED_COURCE" = EXCLUDED."REDUCED_COURCE",
    "CODE_ISO_NUM" = EXCLUDED."CODE_ISO_NUM"
WHERE dse."DATA_ACTUAL_END_DATE" != EXCLUDED."DATA_ACTUAL_END_DATE" OR
      COALESCE(dse."REDUCED_COURCE", 0) != COALESCE(EXCLUDED."REDUCED_COURCE", 0) OR
      COALESCE(dse."CODE_ISO_NUM", 0) != COALESCE(EXCLUDED."CODE_ISO_NUM", 0);


-- ds.md_ledger_account_s
INSERT INTO ds.md_ledger_account_s AS dsl ("CHAPTER", "CHAPTER_NAME", "SECTION_NUMBER", "SECTION_NAME", "SUBSECTION_NAME", "LEDGER1_ACCOUNT", "LEDGER1_ACCOUNT_NAME", "LEDGER_ACCOUNT", "LEDGER_ACCOUNT_NAME", "CHARACTERISTIC", "START_DATE", "END_DATE")
SELECT st."CHAPTER", st."CHAPTER_NAME", st."SECTION_NUMBER", st."SECTION_NAME", st."SUBSECTION_NAME", st."LEDGER1_ACCOUNT", st."LEDGER1_ACCOUNT_NAME", st."LEDGER_ACCOUNT", st."LEDGER_ACCOUNT_NAME", st."CHARACTERISTIC", st."START_DATE"::date, st."END_DATE"::date
FROM stage.md_ledger_account_s AS st
ON CONFLICT ("LEDGER_ACCOUNT", "START_DATE") DO UPDATE
SET "CHAPTER" = EXCLUDED."CHAPTER",
    "CHAPTER_NAME" = EXCLUDED."CHAPTER_NAME",
    "SECTION_NUMBER" = EXCLUDED."SECTION_NUMBER",
    "SECTION_NAME" = EXCLUDED."SECTION_NAME",
    "SUBSECTION_NAME" = EXCLUDED."SUBSECTION_NAME",
    "LEDGER1_ACCOUNT" = EXCLUDED."LEDGER1_ACCOUNT",
    "LEDGER1_ACCOUNT_NAME" = EXCLUDED."LEDGER1_ACCOUNT_NAME",
    "LEDGER_ACCOUNT_NAME" = EXCLUDED."LEDGER_ACCOUNT_NAME",
    "CHARACTERISTIC" = EXCLUDED."CHARACTERISTIC",
    "END_DATE" = EXCLUDED."END_DATE"
WHERE COALESCE(dsl."CHAPTER", '') != COALESCE(EXCLUDED."CHAPTER", '') OR
      COALESCE(dsl."CHAPTER_NAME", '') != COALESCE(EXCLUDED."CHAPTER_NAME", '') OR
      COALESCE(dsl."SECTION_NUMBER", 0) != COALESCE(EXCLUDED."SECTION_NUMBER", 0) OR
      COALESCE(dsl."SECTION_NAME", '') != COALESCE(EXCLUDED."SECTION_NAME", '') OR
      COALESCE(dsl."SUBSECTION_NAME", '') != COALESCE(EXCLUDED."SUBSECTION_NAME", '') OR
      COALESCE(dsl."LEDGER1_ACCOUNT", 0) != COALESCE(EXCLUDED."LEDGER1_ACCOUNT", 0) OR
      COALESCE(dsl."LEDGER1_ACCOUNT_NAME", '') != COALESCE(EXCLUDED."LEDGER1_ACCOUNT_NAME", '') OR
      COALESCE(dsl."LEDGER_ACCOUNT_NAME", '') != COALESCE(EXCLUDED."LEDGER_ACCOUNT_NAME", '') OR
      COALESCE(dsl."CHARACTERISTIC", '') != COALESCE(EXCLUDED."CHARACTERISTIC", '') OR
      dsl."END_DATE" != EXCLUDED."END_DATE";





--таблица для логирования
CREATE TABLE logs.dag_logs (
    log_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    message TEXT,
    operation_start_time TIMESTAMP WITH TIME ZONE,
    operation_end_time TIMESTAMP WITH TIME ZONE
);

DROP TABLE logs.dag_logs;
TRUNCATE TABLE logs.dag_logs RESTART IDENTITY;


--процедура для логирования
CREATE OR REPLACE PROCEDURE logs.insert_dag_log(
    p_dag_id VARCHAR,
    p_task_id VARCHAR,
    p_message TEXT,
    p_operation_start_time TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_operation_end_time TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO logs.dag_logs (
        dag_id, task_id, message,
        operation_start_time, operation_end_time
    )
    VALUES (
        p_dag_id, p_task_id, p_message,
        p_operation_start_time, p_operation_end_time
    );
END;
$$;


DROP PROCEDURE logs.insert_dag_log;




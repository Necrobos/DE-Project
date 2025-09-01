--создание схем
CREATE SCHEMA IF NOT EXISTS DS
CREATE SCHEMA IF NOT EXISTS LOGS
CREATE SCHEMA Stage 



--Удаление данных (для теста пока)
TRUNCATE TABLE stage.ft_balance_f, stage.ft_posting_f, stage.md_account_d, stage.md_currency_d, stage.md_exchange_rate_d, stage.md_ledger_account_s RESTART IDENTITY; 

TRUNCATE TABLE ds.ft_balance_f, ds.ft_posting_f, ds.md_account_d, ds.md_currency_d, ds.md_exchange_rate_d, ds.md_ledger_account_s RESTART IDENTITY;
DROP TABLE IF EXISTS ds.ft_balance_f, ds.ft_posting_f, ds.md_account_d, ds.md_currency_d, ds.md_exchange_rate_d, ds.md_ledger_account_s;


--Проверка дублированных данных
-- Проверка дубликатов
SELECT data_actual_date, currency_rk, COUNT(*)
FROM stage.md_exchange_rate_d
GROUP BY data_actual_date, currency_rk;

-- Удаление дубликатов через ROW_NUMBER
WITH del_dublicate_row AS (
    SELECT currency_rk, data_actual_date,
        ROW_NUMBER() OVER(PARTITION BY currency_rk, data_actual_date ORDER BY (SELECT 1)) row_num
    FROM ds.md_exchange_rate_d
)
DELETE FROM ds.md_exchange_rate_d
WHERE (currency_rk, data_actual_date) IN (
    SELECT currency_rk, data_actual_date 
    FROM del_dublicate_row
    WHERE row_num > 1
);

-- Создание таблиц в схеме ds
CREATE TABLE IF NOT EXISTS ds.ft_balance_f (
    on_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    currency_rk BIGINT,
    balance_out DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f (
    oper_date DATE NOT NULL,
    credit_account_rk BIGINT NOT NULL,
    debet_account_rk BIGINT NOT NULL,
    credit_amount DOUBLE PRECISION,
    debet_amount DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS ds.md_account_d (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE NOT NULL,
    account_rk BIGINT NOT NULL,
    account_number VARCHAR(20) NOT NULL,
    char_type VARCHAR(1) NOT NULL,
    currency_rk BIGINT NOT NULL,
    currency_code VARCHAR(3) NOT NULL
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d (
    currency_rk BIGINT NOT NULL,
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_code VARCHAR(3),
    code_iso_char VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
    data_actual_date DATE NOT NULL,
    data_actual_end_date DATE,
    currency_rk BIGINT NOT NULL,
    reduced_cource DOUBLE PRECISION,
    code_iso_num VARCHAR(3)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
    chapter CHAR(1),
    chapter_name VARCHAR(16),
    section_number INTEGER,
    section_name VARCHAR(22),
    subsection_name VARCHAR(21),
    ledger1_account INTEGER,
    ledger1_account_name VARCHAR(47),
    ledger_account INTEGER NOT NULL,
    ledger_account_name VARCHAR(153),
    characteristic CHAR(1),
    start_date DATE NOT NULL,
    end_date DATE
);

-- Ключи первичные в слое ds
ALTER TABLE ds.ft_balance_f ADD PRIMARY KEY (on_date, account_rk);
ALTER TABLE ds.md_account_d ADD PRIMARY KEY (data_actual_date, account_rk);
ALTER TABLE ds.md_currency_d ADD PRIMARY KEY (currency_rk, data_actual_date);
ALTER TABLE ds.md_ledger_account_s ADD PRIMARY KEY (ledger_account, start_date);
ALTER TABLE ds.md_exchange_rate_d ADD PRIMARY KEY (data_actual_date, currency_rk);

-- Загрузка в схему ds

-- ds.ft_balance_f
CREATE OR REPLACE PROCEDURE ds.insert_ft_balance_f()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ds.ft_balance_f AS dsb (on_date, account_rk, currency_rk, balance_out)
    SELECT st."ON_DATE"::DATE,
           st."ACCOUNT_RK",
           st."CURRENCY_RK",
           st."BALANCE_OUT"
    FROM stage.ft_balance_f AS st
    ON CONFLICT (on_date, account_rk) DO UPDATE
    SET currency_rk = EXCLUDED.currency_rk,
        balance_out = EXCLUDED.balance_out
    WHERE COALESCE(dsb.currency_rk, 0) != COALESCE(EXCLUDED.currency_rk, 0) OR
          COALESCE(dsb.balance_out, 0) != COALESCE(EXCLUDED.balance_out, 0);
    
END;
$$;

-- ds.ft_posting_f
CREATE OR REPLACE PROCEDURE ds.insert_ft_posting_f()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE ds.ft_posting_f;
    
    INSERT INTO ds.ft_posting_f AS dsp (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
    SELECT st."OPER_DATE"::DATE, 
           st."CREDIT_ACCOUNT_RK", 
           st."DEBET_ACCOUNT_RK", 
           st."CREDIT_AMOUNT", 
           st."DEBET_AMOUNT"
    FROM stage.ft_posting_f AS st;
    
END;
$$;

-- ds.md_account_d
CREATE OR REPLACE PROCEDURE ds.insert_md_account_d()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ds.md_account_d AS dsa (data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code)
    SELECT st."DATA_ACTUAL_DATE"::DATE, 
           st."DATA_ACTUAL_END_DATE"::DATE, 
           st."ACCOUNT_RK", 
           st."ACCOUNT_NUMBER", 
           st."CHAR_TYPE", 
           st."CURRENCY_RK", 
           st."CURRENCY_CODE"
    FROM stage.md_account_d AS st
    ON CONFLICT (data_actual_date, account_rk) DO UPDATE
    SET data_actual_end_date = EXCLUDED.data_actual_end_date,
        account_number = EXCLUDED.account_number,
        char_type = EXCLUDED.char_type,
        currency_rk = EXCLUDED.currency_rk,
        currency_code = EXCLUDED.currency_code
    WHERE dsa.data_actual_end_date != EXCLUDED.data_actual_end_date OR
          COALESCE(dsa.account_number, '') != COALESCE(EXCLUDED.account_number, '') OR
          COALESCE(dsa.char_type, '') != COALESCE(EXCLUDED.char_type, '') OR
          COALESCE(dsa.currency_rk, 0) != COALESCE(EXCLUDED.currency_rk, 0) OR
          COALESCE(dsa.currency_code, '') != COALESCE(EXCLUDED.currency_code, '');
    
END;
$$;

-- ds.md_currency_d
CREATE OR REPLACE PROCEDURE ds.insert_md_currency_d()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ds.md_currency_d AS dsc (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
    SELECT st."CURRENCY_RK", 
           st."DATA_ACTUAL_DATE"::DATE, 
           st."DATA_ACTUAL_END_DATE"::DATE, 
           st."CURRENCY_CODE", 
           st."CODE_ISO_CHAR"
    FROM stage.md_currency_d AS st
    ON CONFLICT (currency_rk, data_actual_date) DO UPDATE
    SET data_actual_end_date = EXCLUDED.data_actual_end_date,
        currency_code = EXCLUDED.currency_code,
        code_iso_char = EXCLUDED.code_iso_char
    WHERE dsc.data_actual_end_date != EXCLUDED.data_actual_end_date OR
          COALESCE(dsc.currency_code, '') != COALESCE(EXCLUDED.currency_code, '') OR
          COALESCE(dsc.code_iso_char, '') != COALESCE(EXCLUDED.code_iso_char, '');
END;
$$;

-- ds.md_exchange_rate_d
CREATE OR REPLACE PROCEDURE ds.insert_md_exchange_rate_d()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ds.md_exchange_rate_d AS dse (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
    SELECT st."DATA_ACTUAL_DATE"::DATE, 
           st."DATA_ACTUAL_END_DATE"::DATE, 
           st."CURRENCY_RK", 
           st."REDUCED_COURCE", 
           st."CODE_ISO_NUM"
    FROM stage.md_exchange_rate_d AS st
    ON CONFLICT (data_actual_date, currency_rk) DO UPDATE
    SET data_actual_end_date = EXCLUDED.data_actual_end_date,
        reduced_cource = EXCLUDED.reduced_cource,
        code_iso_num = EXCLUDED.code_iso_num
    WHERE dse.data_actual_end_date != EXCLUDED.data_actual_end_date OR
          COALESCE(dse.reduced_cource, 0) != COALESCE(EXCLUDED.reduced_cource, 0) OR
          COALESCE(dse.code_iso_num, '') != COALESCE(EXCLUDED.code_iso_num, '');
    
END;
$$;

-- ds.md_ledger_account_s
CREATE OR REPLACE PROCEDURE ds.insert_md_ledger_account_s()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO ds.md_ledger_account_s AS dsl (chapter, chapter_name, section_number, section_name, subsection_name, ledger1_account, ledger1_account_name, ledger_account, ledger_account_name, characteristic, start_date, end_date)
    SELECT st."CHAPTER", 
           st."CHAPTER_NAME", 
           st."SECTION_NUMBER", 
           st."SECTION_NAME", 
           st."SUBSECTION_NAME", 
           st."LEDGER1_ACCOUNT", 
           st."LEDGER1_ACCOUNT_NAME", 
           st."LEDGER_ACCOUNT", 
           st."LEDGER_ACCOUNT_NAME", 
           st."CHARACTERISTIC", 
           st."START_DATE"::DATE, 
           st."END_DATE"::DATE
    FROM stage.md_ledger_account_s AS st
    ON CONFLICT (ledger_account, start_date) DO UPDATE
    SET chapter = EXCLUDED.chapter,
        chapter_name = EXCLUDED.chapter_name,
        section_number = EXCLUDED.section_number,
        section_name = EXCLUDED.section_name,
        subsection_name = EXCLUDED.subsection_name,
        ledger1_account = EXCLUDED.ledger1_account,
        ledger1_account_name = EXCLUDED.ledger1_account_name,
        ledger_account_name = EXCLUDED.ledger_account_name,
        characteristic = EXCLUDED.characteristic,
        end_date = EXCLUDED.end_date
    WHERE COALESCE(dsl.chapter, '') != COALESCE(EXCLUDED.chapter, '') OR
          COALESCE(dsl.chapter_name, '') != COALESCE(EXCLUDED.chapter_name, '') OR
          COALESCE(dsl.section_number, 0) != COALESCE(EXCLUDED.section_number, 0) OR
          COALESCE(dsl.section_name, '') != COALESCE(EXCLUDED.section_name, '') OR
          COALESCE(dsl.subsection_name, '') != COALESCE(EXCLUDED.subsection_name, '') OR
          COALESCE(dsl.ledger1_account, 0) != COALESCE(EXCLUDED.ledger1_account, 0) OR
          COALESCE(dsl.ledger1_account_name, '') != COALESCE(EXCLUDED.ledger1_account_name, '') OR
          COALESCE(dsl.ledger_account_name, '') != COALESCE(EXCLUDED.ledger_account_name, '') OR
          COALESCE(dsl.characteristic, '') != COALESCE(EXCLUDED.characteristic, '') OR
          dsl.end_date != EXCLUDED.end_date;
END;
$$;


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

TRUNCATE TABLE logs.dag_logs RESTART IDENTITY;
DROP PROCEDURE logs.insert_dag_log;


SELECT * FROM ds.ft_balance_f
WHERE account_rk = 24656;

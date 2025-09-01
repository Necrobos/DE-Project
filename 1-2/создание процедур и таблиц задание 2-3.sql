CREATE SCHEMA DM;


--скрипт создания таблиц
CREATE TABLE dm.dm_account_turnover_f (
    on_date DATE,
    account_rk DECIMAL,
    credit_amount DECIMAL(23,8),
    credit_amount_rub DECIMAL(23,8),
    debet_amount DECIMAL(23,8),
    debet_amount_rub DECIMAL(23,8)
);

CREATE TABLE dm.dm_account_balance_f (
    on_date DATE,
    account_rk DECIMAL,
    balance_out DECIMAL(23,8),
    balance_out_rub DECIMAL(23,8)
);

CREATE TABLE dm.dm_f101_round_f (
    from_date DATE,
    to_date DATE,
    chapter CHAR(1),
    ledger_account CHAR(5),
    characteristic CHAR(1),
    balance_in_rub DECIMAL(23,8),
    balance_in_val DECIMAL(23,8),
    balance_in_total DECIMAL(23,8),
    turn_deb_rub DECIMAL(23,8),
    turn_deb_val DECIMAL(23,8),
    turn_deb_total DECIMAL(23,8),
    turn_cre_rub DECIMAL(23,8),
    turn_cre_val DECIMAL(23,8),
    turn_cre_total DECIMAL(23,8),
    balance_out_rub DECIMAL(23,8),
    balance_out_val DECIMAL(23,8),
    balance_out_total DECIMAL(23,8)
);


DROP TABLE IF EXISTS dm.dm_account_turnover_f, dm.dm_account_balance_f, dm.dm_f101_round_f, dm.dm_f101_round_f_v2;


CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(
    i_ondate DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM dm.dm_account_turnover_f 
    WHERE on_date = i_ondate;
    
    INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    
    SELECT i_ondate,
            account_rk,
            COALESCE(SUM(credit_amount), 0),
            COALESCE(SUM(credit_amount_rub), 0),
            COALESCE(SUM(debet_amount), 0),
            COALESCE(SUM(debet_amount_rub), 0)
    FROM (
        -- Кредитовые обороты
        SELECT 
            dsp.credit_account_rk as account_rk,
            dsp.credit_amount as credit_amount,
            dsp.credit_amount * COALESCE(er.reduced_cource, 1) as credit_amount_rub,
            0 as debet_amount,
            0 as debet_amount_rub
        FROM ds.ft_posting_f dsp
        LEFT JOIN ds.md_account_d acc ON acc.account_rk = dsp.credit_account_rk 
            AND i_ondate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
        LEFT JOIN ds.md_exchange_rate_d er ON er.currency_rk = acc.currency_rk
            AND i_ondate BETWEEN er.data_actual_date AND er.data_actual_end_date
        WHERE dsp.oper_date = i_ondate
        
        UNION ALL
        
        -- Дебетовые обороты
        SELECT 
            dsp.debet_account_rk as account_rk,
            0 as credit_amount,
            0 as credit_amount_rub,
            dsp.debet_amount as debet_amount,
            dsp.debet_amount * COALESCE(er.reduced_cource, 1) as debet_amount_rub
        FROM ds.ft_posting_f dsp
        LEFT JOIN ds.md_account_d acc ON acc.account_rk = dsp.debet_account_rk 
            AND i_ondate BETWEEN acc.data_actual_date AND acc.data_actual_end_date
        LEFT JOIN ds.md_exchange_rate_d er ON er.currency_rk = acc.currency_rk
            AND i_ondate BETWEEN er.data_actual_date AND er.data_actual_end_date
        WHERE dsp.oper_date = i_ondate
    ) combined_data
    GROUP BY account_rk
    HAVING COALESCE(SUM(credit_amount), 0) + COALESCE(SUM(debet_amount), 0) > 0;
END
$$;



CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(
    i_OnDate DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

	INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
		SELECT '2017-12-31' AS on_date,
		    fb.account_rk,
		    fb.balance_out,
		    fb.balance_out * COALESCE(er.reduced_cource, 1) as balance_out_rub
		FROM ds.ft_balance_f fb
		LEFT JOIN ds.md_account_d acc ON acc.account_rk = fb.account_rk 
		    AND '2017-12-31' BETWEEN acc.data_actual_date AND acc.data_actual_end_date
		LEFT JOIN ds.md_exchange_rate_d er ON er.currency_rk = acc.currency_rk 
		    AND '2017-12-31' BETWEEN er.data_actual_date AND er.data_actual_end_date
		WHERE fb.on_date = '2017-12-31';
    
    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT 
        i_OnDate,
        acc.account_rk,
        CASE acc.char_type
            WHEN 'А' THEN -- Активный счет
                COALESCE(prev_bal.balance_out, 0) + 
                COALESCE(turn.debet_amount, 0) - 
                COALESCE(turn.credit_amount, 0)
            WHEN 'П' THEN -- Пассивный счет
                COALESCE(prev_bal.balance_out, 0) - 
                COALESCE(turn.debet_amount, 0) + 
                COALESCE(turn.credit_amount, 0)
            ELSE COALESCE(prev_bal.balance_out, 0)
        END as balance_out,
        
        CASE acc.char_type
            WHEN 'А' THEN -- Активный счет
                COALESCE(prev_bal.balance_out_rub, 0) + 
                COALESCE(turn.debet_amount_rub, 0) - 
                COALESCE(turn.credit_amount_rub, 0)
            WHEN 'П' THEN -- Пассивный счет
                COALESCE(prev_bal.balance_out_rub, 0) - 
                COALESCE(turn.debet_amount_rub, 0) + 
                COALESCE(turn.credit_amount_rub, 0)
            ELSE COALESCE(prev_bal.balance_out_rub, 0)
        END as balance_out_rub
    FROM ds.md_account_d acc
	
    LEFT JOIN dm.dm_account_balance_f prev_bal 
        ON prev_bal.account_rk = acc.account_rk 
        AND prev_bal.on_date = i_OnDate - INTERVAL '1 day'
    LEFT JOIN dm.dm_account_turnover_f turn 
        ON turn.account_rk = acc.account_rk 
        AND turn.on_date = i_OnDate	
		
    WHERE i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;
END;
$$;




TRUNCATE TABLE dm.dm_account_balance_f, dm.dm_account_turnover_f;




CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f (
	i_OnDate DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_StartDate DATE;
    v_EndDate DATE;
    v_PrevDate DATE;
BEGIN
    -- Определяем даты отчетного периода
    v_StartDate := (i_OnDate - INTERVAL '1 month')::DATE;
    v_EndDate := (i_OnDate - INTERVAL '1 day')::DATE;
    v_PrevDate := (v_StartDate - INTERVAL '1 day')::DATE;
    
    DELETE FROM dm.dm_f101_round_f 
    WHERE from_date = v_StartDate AND to_date = v_EndDate;
    
    -- Вставляем новые данные
    INSERT INTO dm.dm_f101_round_f (
        from_date,
        to_date,
        chapter,
        ledger_account,
        characteristic,
        balance_in_rub,
        balance_in_val,
        balance_in_total,
        turn_deb_rub,
        turn_deb_val,
        turn_deb_total,
        turn_cre_rub,
        turn_cre_val,
        turn_cre_total,
        balance_out_rub,
        balance_out_val,
        balance_out_total
    )
    WITH account_info AS (
        SELECT 
            a.account_rk,
            SUBSTRING(a.account_number FROM 1 FOR 5) AS ledger_account,
            a.char_type AS characteristic,
            a.currency_code,
            las.chapter
        FROM ds.md_account_d a
        LEFT JOIN ds.md_ledger_account_s las ON CAST(las.ledger_account AS VARCHAR) = SUBSTRING(a.account_number FROM 1 FOR 5)
        WHERE a.data_actual_date <= v_EndDate
          AND a.data_actual_end_date >= v_StartDate
    ),
    beginning_balances AS (
        SELECT 
            ai.ledger_account,
            ai.characteristic,
            ai.chapter,
            SUM(CASE WHEN ai.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN ai.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_val,
            SUM(b.balance_out_rub) AS balance_in_total
        FROM dm.dm_account_balance_f b
        JOIN account_info ai ON b.account_rk = ai.account_rk
        WHERE b.on_date = v_PrevDate
        GROUP BY ai.ledger_account, ai.characteristic, ai.chapter
    ),
    turnovers AS (
        SELECT 
            ai.ledger_account,
            ai.characteristic,
            ai.chapter,
            SUM(CASE WHEN ai.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN ai.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
            SUM(t.debet_amount_rub) AS turn_deb_total,
            SUM(CASE WHEN ai.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN ai.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
            SUM(t.credit_amount_rub) AS turn_cre_total
        FROM dm.dm_account_turnover_f t
        JOIN account_info ai ON t.account_rk = ai.account_rk
        WHERE t.on_date BETWEEN v_StartDate AND v_EndDate
        GROUP BY ai.ledger_account, ai.characteristic, ai.chapter
    ),
    ending_balances AS (
        SELECT 
            ai.ledger_account,
            ai.characteristic,
            ai.chapter,
            SUM(CASE WHEN ai.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN ai.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_out_val,
            SUM(b.balance_out_rub) AS balance_out_total
        FROM dm.dm_account_balance_f b
        JOIN account_info ai ON b.account_rk = ai.account_rk
        WHERE b.on_date = v_EndDate
        GROUP BY ai.ledger_account, ai.characteristic, ai.chapter
    )
    SELECT 
        v_StartDate AS from_date,
        v_EndDate AS to_date,
        COALESCE(bb.chapter, t.chapter, eb.chapter) AS chapter,
        COALESCE(bb.ledger_account, t.ledger_account, eb.ledger_account) AS ledger_account,
        COALESCE(bb.characteristic, t.characteristic, eb.characteristic) AS characteristic,
        COALESCE(bb.balance_in_rub, 0) AS balance_in_rub,
        COALESCE(bb.balance_in_val, 0) AS balance_in_val,
        COALESCE(bb.balance_in_total, 0) AS balance_in_total,
        COALESCE(t.turn_deb_rub, 0) AS turn_deb_rub,
        COALESCE(t.turn_deb_val, 0) AS turn_deb_val,
        COALESCE(t.turn_deb_total, 0) AS turn_deb_total,
        COALESCE(t.turn_cre_rub, 0) AS turn_cre_rub,
        COALESCE(t.turn_cre_val, 0) AS turn_cre_val,
        COALESCE(t.turn_cre_total, 0) AS turn_cre_total,
        COALESCE(eb.balance_out_rub, 0) AS balance_out_rub,
        COALESCE(eb.balance_out_val, 0) AS balance_out_val,
        COALESCE(eb.balance_out_total, 0) AS balance_out_total
    FROM beginning_balances bb
    FULL JOIN turnovers t ON bb.ledger_account = t.ledger_account 
        AND bb.characteristic = t.characteristic 
        AND bb.chapter = t.chapter
    FULL JOIN ending_balances eb ON COALESCE(bb.ledger_account, t.ledger_account) = eb.ledger_account 
        AND COALESCE(bb.characteristic, t.characteristic) = eb.characteristic 
        AND COALESCE(bb.chapter, t.chapter) = eb.chapter;
    
END;
$$;



SELECT * FROM rd.copy_account_balance

--корректная сумма выходной вчерашний остаток
SELECT ab.account_rk, ab.effective_date, ab.account_out_sum, ab2.effective_date, ab2.account_in_sum, ab.account_out_sum AS correct_sum
FROM rd.account_balance ab
JOIN rd.account_balance ab2
	ON ab2.effective_date = ab.effective_date + INTERVAL '1 day'
	AND ab.account_rk = ab2.account_rk
WHERE ab2.account_in_sum <> ab.account_out_sum

--корректная сумма входной сегодняшний остаток
SELECT ab.account_rk, ab.effective_date, ab.account_out_sum, ab2.effective_date, ab2.account_in_sum , ab2.account_in_sum AS correct_sum
FROM rd.account_balance ab
JOIN rd.account_balance ab2
	ON ab2.effective_date = ab.effective_date + INTERVAL '1 day'
	AND ab.account_rk = ab2.account_rk
WHERE ab2.account_in_sum <> ab.account_out_sum


CREATE TABLE rd.copy_account_balance AS
SELECT * FROM rd.account_balance;

DROP TABLE IF EXISTS rd.copy_account_balance;

--update для корректного остатка вчерашнего дня
CREATE OR REPLACE PROCEDURE account_balance_correct_out_sum()
LANGUAGE plpgsql
AS $$
BEGIN
	UPDATE rd.account_balance AS cur_day
	SET account_in_sum = prev_day.account_out_sum
	FROM rd.account_balance prev_day
	WHERE cur_day.effective_date =  prev_day.effective_date + INTERVAL '1 day'
		AND cur_day.account_rk = prev_day.account_rk
		AND cur_day.account_in_sum <> prev_day.account_out_sum;
END;
$$;


--update для корректного остатка сегодняшнего дня
CREATE OR REPLACE PROCEDURE account_balance_correct_in_sum()
LANGUAGE plpgsql
AS $$
BEGIN
	UPDATE rd.account_balance AS prev_day
	SET account_out_sum = cur_day.account_in_sum
	FROM rd.account_balance cur_day
	WHERE cur_day.effective_date = prev_day.effective_date + INTERVAL '1 day'
		AND cur_day.account_rk = prev_day.account_rk
		AND cur_day.account_in_sum <> prev_day.account_out_sum;
END;
$$;
	
CALL account_balance_correct_out_sum();
CALL account_balance_correct_in_sum();





--прототип витрины balance_turnover
SELECT a.account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd




CREATE OR REPLACE PROCEDURE insert_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
	TRUNCATE TABLE dm.account_balance_turnover;

	INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
	SELECT a.account_rk,
	   COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
	   a.department_rk,
	   ab.effective_date,
	   ab.account_in_sum,
	   ab.account_out_sum
FROM rd.account a
LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
END;
$$;

CALL insert_balance_turnover();

--тест по ключу
SELECT * FROM dm.account_balance_turnover
WHERE account_rk = 2943625;
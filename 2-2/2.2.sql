CREATE SCHEMA stage;

--TRUNCATE
--INSERT INTO
with deal as (
select  deal_rk
	   ,deal_num --Номер сделки
	   ,deal_name --Наименование сделки
	   ,deal_sum --Сумма сделки
	   ,client_rk --Ссылка на клиента
	   ,agreement_rk --Ссылка на договор
	   ,deal_start_date --Дата начала действия сделки
	   ,department_rk --Ссылка на отделение
	   ,product_rk -- Ссылка на продукт
	   ,deal_type_cd
	   ,effective_from_date
	   ,effective_to_date
from RD.deal_info
), loan_holiday as (
select  deal_rk
	   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
	   ,loan_holiday_start_date     --Дата начала кредитных каникул
	   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
	   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
	   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
	   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
	   ,effective_from_date
	   ,effective_to_date
from RD.loan_holiday
), product as (
select product_rk
	  ,product_name
	  ,effective_from_date
	  ,effective_to_date
from RD.product
), holiday_info as (
select   d.deal_rk
        ,lh.effective_from_date
        ,lh.effective_to_date
        ,d.deal_num as deal_number --Номер сделки
	    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
        ,d.deal_name --Наименование сделки
        ,d.deal_sum --Сумма сделки
        ,d.client_rk --Ссылка на контрагента
        ,d.agreement_rk --Ссылка на договор
        ,d.deal_start_date --Дата начала действия сделки
        ,d.department_rk --Ссылка на ГО/филиал
        ,d.product_rk -- Ссылка на продукт
        ,p.product_name -- Наименование продукта
        ,d.deal_type_cd -- Наименование типа сделки
from deal d
left join loan_holiday lh on 1=1
                             and d.deal_rk = lh.deal_rk
                             and d.effective_from_date = lh.effective_from_date
left join product p on p.product_rk = d.product_rk
					   and p.effective_from_date = d.effective_from_date
)
SELECT deal_rk
      ,effective_from_date
      ,effective_to_date
      ,agreement_rk
      ,client_rk
      ,department_rk
      ,product_rk
      ,product_name
      ,deal_type_cd
      ,deal_start_date
      ,deal_name
      ,deal_number
      ,deal_sum
      ,loan_holiday_type_cd
      ,loan_holiday_start_date
      ,loan_holiday_finish_date
      ,loan_holiday_fact_finish_date
      ,loan_holiday_finish_flg
      ,loan_holiday_last_possible_date
FROM holiday_info;



ALTER TABLE rd.product ADD PRIMARY KEY (product_rk, effective_from_date);
ALTER TABLE rd.deal_info ADD PRIMARY KEY (deal_rk, effective_from_date);
ALTER TABLE dm.dict_currency ADD PRIMARY KEY (currency_cd, currency_name);

-- SELECT * FROM rd.deal_info
-- WHERE (deal_rk, effective_from_date)=(4531242, '2023-08-11');

-- SELECT * FROM rd.loan_holiday
-- WHERE (deal_rk, effective_from_date)=(4531242, '2023-08-11');

-- SELECT DISTINCT loan_holiday_last_possible_date
-- FROM rd.loan_holiday;


--Для чистки deal_info
CREATE OR REPLACE PROCEDURE del_dubl_rd_deal_info()
LANGUAGE plpgsql
AS $$
BEGIN
WITH check_dubl AS (
	SELECT ctid,
	ROW_NUMBER() OVER (PARTITION BY deal_rk, effective_from_date ORDER BY deal_start_date DESC) AS row_num
	FROM rd.deal_info
)
DELETE FROM rd.deal_info
WHERE ctid IN (
	SELECT ctid
	FROM check_dubl
	WHERE row_num > 1);
END;
$$;

--Для чистки product
CREATE OR REPLACE PROCEDURE del_dubl_rd_product_info()
LANGUAGE plpgsql
AS $$
BEGIN
WITH check_dubl AS (
	SELECT *, ctid,
	ROW_NUMBER() OVER (PARTITION BY product_rk, effective_from_date ORDER BY product_name DESC) AS row_num
	FROM rd.product
)

DELETE FROM rd.product
WHERE ctid IN (
	SELECT ctid
	FROM check_dubl
	WHERE row_num > 1
	);
END;
$$;



CREATE OR REPLACE PROCEDURE insert_rd_deal()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO rd.deal_info AS di(deal_rk, deal_num, deal_name, deal_sum, client_rk, account_rk, agreement_rk, deal_start_date, department_rk, product_rk, deal_type_cd, effective_from_date, effective_to_date)
    SELECT 
        st."deal_rk"::BIGINT,   
        st."deal_num"::TEXT,             
        st."deal_name"::TEXT,            
        st."deal_sum"::NUMERIC,            
        st."client_rk"::BIGINT,           
        st."account_rk"::BIGINT,           
        st."agreement_rk"::BIGINT,       
        st."deal_start_date"::DATE,      
        st."department_rk"::BIGINT,        
        st."product_rk"::BIGINT,           
        st."deal_type_cd"::TEXT,         
        st."effective_from_date"::DATE,   
        st."effective_to_date"::DATE      
    FROM stage.deal_info AS st
    ON CONFLICT (deal_rk, effective_from_date) DO UPDATE
    SET deal_num = EXCLUDED.deal_num,
        deal_name = EXCLUDED.deal_name,
        deal_sum = EXCLUDED.deal_sum,
        client_rk = EXCLUDED.client_rk,
        account_rk = EXCLUDED.account_rk,
        agreement_rk = EXCLUDED.agreement_rk,
        deal_start_date = EXCLUDED.deal_start_date,
        department_rk = EXCLUDED.department_rk,
        product_rk = EXCLUDED.product_rk,
        deal_type_cd = EXCLUDED.deal_type_cd,
        effective_from_date = EXCLUDED.effective_from_date,
        effective_to_date = EXCLUDED.effective_to_date
    WHERE COALESCE(di.deal_num, '') != COALESCE(EXCLUDED.deal_num, '') OR
          COALESCE(di.deal_name, '') != COALESCE(EXCLUDED.deal_name, '') OR
          COALESCE(di.deal_sum, 0) != COALESCE(EXCLUDED.deal_sum, 0) OR
          COALESCE(di.client_rk, 0) != COALESCE(EXCLUDED.client_rk, 0) OR
          COALESCE(di.account_rk, 0) != COALESCE(EXCLUDED.account_rk, 0) OR
          COALESCE(di.agreement_rk, 0) != COALESCE(EXCLUDED.agreement_rk, 0) OR
          COALESCE(di.deal_start_date::TEXT, '') != COALESCE(EXCLUDED.deal_start_date::TEXT, '') OR
          COALESCE(di.department_rk, 0) != COALESCE(EXCLUDED.department_rk, 0) OR
          COALESCE(di.product_rk, 0) != COALESCE(EXCLUDED.product_rk, 0) OR
          COALESCE(di.deal_type_cd, '') != COALESCE(EXCLUDED.deal_type_cd, '') OR
          COALESCE(di.effective_from_date::TEXT, '') != COALESCE(EXCLUDED.effective_from_date::TEXT, '') OR
          COALESCE(di.effective_to_date::TEXT, '') != COALESCE(EXCLUDED.effective_to_date::TEXT, '');
END;
$$;


CREATE OR REPLACE PROCEDURE insert_product_info()
LANGUAGE plpgsql
AS $$
BEGIN 
	INSERT INTO rd.product AS p (product_rk,product_name, effective_from_date, effective_to_date)
	SELECT 
		st.product_rk::BIGINT,
		st.product_name::TEXT,
		st.effective_from_date::DATE,
		st.effective_to_date::DATE
	FROM stage.product_info AS st
	ON CONFLICT (product_rk, effective_from_date) DO UPDATE
	SET product_name = EXCLUDED.product_name,
		effective_to_date = EXCLUDED.effective_to_date
	WHERE COALESCE(p.product_name, '') != COALESCE(EXCLUDED.product_name, '') OR
		  COALESCE(p.effective_to_date::TEXT, '') != COALESCE(EXCLUDED.effective_to_date::TEXT, '');
END;
$$;


CREATE OR REPLACE PROCEDURE insert_dict_currency()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dm.dict_currency AS d (currency_cd, currency_name, effective_from_date, effective_to_date)
    SELECT
        st.currency_cd::TEXT,
        st.currency_name::TEXT,
        st.effective_from_date::DATE,
        st.effective_to_date::DATE
    FROM stage.dict_currency AS st
    ON CONFLICT (currency_cd, currency_name) DO UPDATE
    SET effective_to_date = EXCLUDED.effective_to_date,
		effective_from_date = EXCLUDED.effective_from_date
    WHERE COALESCE(d.effective_to_date::TEXT, '') != COALESCE(EXCLUDED.effective_to_date::TEXT, '') OR
          COALESCE(d.effective_from_date::TEXT, '') != COALESCE(EXCLUDED.effective_from_date::TEXT, '');
END;
$$;

CALL insert_dict_currency();

SELECT COUNT(*) FROM stage.deal_info;
SELECT DISTINCT deal_rk FROM stage.deal_info;


SELECT COUNT(*) FROM stage.product_info;
SELECT DISTINCT (product_rk, effective_from_date) FROM stage.product_info;


CREATE OR REPLACE PROCEDURE del_dubl_stage_deal_info()
LANGUAGE plpgsql
AS $$
BEGIN
	WITH check_dubl AS (
		SELECT ctid,
		ROW_NUMBER() OVER (PARTITION BY deal_rk, effective_from_date ORDER BY deal_start_date DESC) AS row_num
		FROM stage.deal_info
	)
	DELETE FROM stage.deal_info
	WHERE ctid IN (
		SELECT ctid
		FROM check_dubl
		WHERE row_num > 1);
END;
$$;

CREATE OR REPLACE PROCEDURE del_dubl_stage_product_info()
LANGUAGE plpgsql
AS $$
BEGIN
WITH check_dubl AS (
	SELECT *, ctid,
	ROW_NUMBER() OVER (PARTITION BY product_rk, effective_from_date ORDER BY product_name DESC) AS row_num
	FROM stage.product_info
)

DELETE FROM stage.product_info
WHERE ctid IN (
	SELECT ctid
	FROM check_dubl
	WHERE row_num > 1
	);
END;
$$;

CALL rd.insert_rd_deal();

SELECT COUNT(*) FROM rd.deal_info


CREATE OR REPLACE PROCEDURE build_prototype_2_2()
LANGUAGE plpgsql
AS $$
BEGIN
	TRUNCATE TABLE dm.loan_holiday_info RESTART IDENTITY;
	
	INSERT INTO dm.loan_holiday_info (
		deal_rk, effective_from_date, effective_to_date,
		agreement_rk, account_rk, client_rk,
		department_rk, product_rk, product_name,
		deal_type_cd, deal_start_date, deal_name,
		deal_number, deal_sum, loan_holiday_type_cd,
		loan_holiday_start_date, loan_holiday_finish_date, loan_holiday_fact_finish_date,
		loan_holiday_finish_flg, loan_holiday_last_possible_date
	) 
		with deal as (
	select  deal_rk
		   ,deal_num --Номер сделки
		   ,deal_name --Наименование сделки
		   ,deal_sum --Сумма сделки
		   ,account_rk
		   ,client_rk --Ссылка на клиента
		   ,agreement_rk --Ссылка на договор
		   ,deal_start_date --Дата начала действия сделки
		   ,department_rk --Ссылка на отделение
		   ,product_rk -- Ссылка на продукт
		   ,deal_type_cd
		   ,effective_from_date
		   ,effective_to_date
	from RD.deal_info
	), loan_holiday as (
	select  deal_rk
		   ,loan_holiday_type_cd  --Ссылка на тип кредитных каникул
		   ,loan_holiday_start_date     --Дата начала кредитных каникул
		   ,loan_holiday_finish_date    --Дата окончания кредитных каникул
		   ,loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
		   ,loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
		   ,loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
		   ,effective_from_date
		   ,effective_to_date
	from RD.loan_holiday
	), product as (
	select product_rk
		  ,product_name
		  ,effective_from_date
		  ,effective_to_date
	from RD.product
	), holiday_info as (
	select   d.deal_rk
	        ,lh.effective_from_date
	        ,lh.effective_to_date
	        ,d.deal_num as deal_number --Номер сделки
		    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
	        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
	        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
	        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
	        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
	        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
	        ,d.deal_name --Наименование сделки
	        ,d.deal_sum --Сумма сделки
	        ,d.client_rk --Ссылка на контрагента
			,d.account_rk
	        ,d.agreement_rk --Ссылка на договор
	        ,d.deal_start_date --Дата начала действия сделки
	        ,d.department_rk --Ссылка на ГО/филиал
	        ,d.product_rk -- Ссылка на продукт
	        ,p.product_name -- Наименование продукта
	        ,d.deal_type_cd -- Наименование типа сделки
	from deal d
	left join loan_holiday lh on 1=1
	                             and d.deal_rk = lh.deal_rk
	                             and d.effective_from_date = lh.effective_from_date
	left join product p on p.product_rk = d.product_rk
						   and p.effective_from_date = d.effective_from_date
	)
	SELECT deal_rk
	      ,effective_from_date
	      ,effective_to_date
	      ,agreement_rk
		  ,account_rk
	      ,client_rk
	      ,department_rk
	      ,product_rk
	      ,product_name
	      ,deal_type_cd
	      ,deal_start_date
	      ,deal_name
	      ,deal_number
	      ,deal_sum
	      ,loan_holiday_type_cd
	      ,loan_holiday_start_date
	      ,loan_holiday_finish_date
	      ,loan_holiday_fact_finish_date
	      ,loan_holiday_finish_flg
	      ,loan_holiday_last_possible_date
	FROM holiday_info;
END;
$$;

CALL build_prototype_2_2();
WITH del_dup AS(
	SELECT ctid, client_rk, effective_from_date,
		ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date ORDER BY effective_from_date) AS count_dup
		FROM dm.client
)
DELETE FROM dm.client
WHERE ctid IN
(
	SELECT ctid
	FROM del_dup
	WHERE count_dup > 1
);

SELECT * FROM dm.client;
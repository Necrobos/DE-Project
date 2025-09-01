-- Создание таблицы client
CREATE TABLE dm.client (
    client_rk INT,
    effective_from_date TIMESTAMP,
    effective_to_date TIMESTAMP,
    account_rk INT,
    address_rk INT,
    department_rk INT,
    card_type_code VARCHAR(255),
    client_id VARCHAR(255),
    counterparty_type_cd VARCHAR(255),
    black_list_flag BOOLEAN,
    client_open_dttm TIMESTAMP,
    bankruptcy_rk INT
);

DROP TABLE dm.client;

-- Вставка 7 уникальных записей
INSERT INTO dm.client (client_rk, effective_from_date, effective_to_date, account_rk, address_rk, department_rk, card_type_code, client_id, counterparty_type_cd, black_list_flag, client_open_dttm, bankruptcy_rk) VALUES
(1, '2023-01-01 00:00:00', '2024-01-01 00:00:00', 1001, 2001, 3001, 'VISA', 'C123', 'TYPE_A', TRUE, '2023-01-01 10:00:00', 4001),
(2, '2023-02-01 00:00:00', '2024-02-01 00:00:00', 1002, 2002, 3002, 'MASTERCARD', 'C124', 'TYPE_B', FALSE, '2023-02-01 11:00:00', 4002),
(3, '2023-03-01 00:00:00', '2024-03-01 00:00:00', 1003, 2003, 3003, 'AMEX', 'C125', 'TYPE_C', TRUE, '2023-03-01 12:00:00', 4003),
(4, '2023-04-01 00:00:00', '2024-04-01 00:00:00', 1004, 2004, 3004, 'VISA', 'C126', 'TYPE_D', FALSE, '2023-04-01 13:00:00', 4004),
(5, '2023-05-01 00:00:00', '2024-05-01 00:00:00', 1005, 2005, 3005, 'MASTERCARD', 'C127', 'TYPE_E', TRUE, '2023-05-01 14:00:00', 4005),
(6, '2023-06-01 00:00:00', '2024-06-01 00:00:00', 1006, 2006, 3006, 'VISA', 'C128', 'TYPE_F', FALSE, '2023-06-01 15:00:00', 4006),
(7, '2023-07-01 00:00:00', '2024-07-01 00:00:00', 1007, 2007, 3007, 'MASTERCARD', 'C129', 'TYPE_G', TRUE, '2023-07-01 16:00:00', 4007);

-- Вставка 3 дубликатов
INSERT INTO dm.client (client_rk, effective_from_date, effective_to_date, account_rk, address_rk, department_rk, card_type_code, client_id, counterparty_type_cd, black_list_flag, client_open_dttm, bankruptcy_rk) VALUES
(1, '2023-01-01 00:00:00', '2024-01-01 00:00:00', 1001, 2001, 3001, 'VISA', 'C123', 'TYPE_A', TRUE, '2023-01-01 10:00:00', 4001), -- Дубликат записи 1
(2, '2023-02-01 00:00:00', '2024-02-01 00:00:00', 1002, 2002, 3002, 'MASTERCARD', 'C124', 'TYPE_B', FALSE, '2023-02-01 11:00:00', 4002), -- Дубликат записи 2
(3, '2023-03-01 00:00:00', '2024-03-01 00:00:00', 1003, 2003, 3003, 'AMEX', 'C125', 'TYPE_C', TRUE, '2023-03-01 12:00:00', 4003); -- Дубликат записи 3

-- Вывод всех записей (для проверки)
SELECT * FROM dm.client;






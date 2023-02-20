BEGIN TRANSACTION;
SELECT
    123234 as c,
    $session_time as session_time,
    '${my_var}',
    TO_TIMESTAMP_TZ(1639668288), // 2021/12/17 00:24:48 JST
    1639668288::TIMESTAMP_TZ
;
COMMIT;
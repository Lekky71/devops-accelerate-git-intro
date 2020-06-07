FROM (
    SELECT
      sms.id,
      mambu_client_id,
      sms.thread_id,
      sms.message,
      sms.date,
      CASE
        WHEN REGEXP_EXTRACT(message, r"(?i)NGN([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)NGN([\d,]+\.\d{0,2})"), ",", "" ) AS FLOAT64 )
        WHEN REGEXP_EXTRACT(message, r"(?i)NGN ([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)NGN ([\d,]+\.\d{0,2})"), ",", "") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)NGN([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)NGN([\d]+\.\d{0,2})") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)NGN ([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)NGN ([\d]+\.\d{0,2})") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)N([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)N([\d,]+\.\d{0,2})"), ",", "") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)N ([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)N ([\d,]+\.\d{0,2})"), ",", "") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)N([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)N([\d]+\.\d{0,2})") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)N ([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)N ([\d]+\.\d{0,2})") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt:([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)Amt:([\d,]+\.\d{0,2})"), ",", "") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt: ([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)Amt: ([\d,]+\.\d{0,2})"), ",", "") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt:([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)Amt:([\d]+\.\d{0,2})") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt: ([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)Amt: ([\d]+\.\d{0,2})") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)Amt([\d,]+\.\d{0,2})"), ",", "") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt ([\d,]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REPLACE(REGEXP_EXTRACT(message, r"(?i)Amt ([\d,]+\.\d{0,2})"), ",", "") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)Amt([\d]+\.\d{0,2})") AS FLOAT64)
        WHEN REGEXP_EXTRACT(message, r"(?i)Amt ([\d]+\.\d{0,2})") IS NOT NULL THEN SAFE_CAST(REGEXP_EXTRACT(message, r"(?i)Amt ([\d]+\.\d{0,2})") AS FLOAT64)
      ELSE
      0
    END
      AS value_txn,
      ROW_NUMBER() OVER(PARTITION BY sms.id ORDER BY sms.date DESC) AS reg_order
    FROM
      `gold-courage-194810.sdk.sms` sms
    JOIN
      `sdk.client_info`
    USING
      (registered_installation_id)
    JOIN
      nigeria_clients kc
    ON
      mambu_client_id = kc.id
    WHERE
      type='inbox'
      AND (REGEXP_EXTRACT(message, r"(?i)NGN([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)NGN ([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)NGN([\d]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)NGN ([\d]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)N([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)N ([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)N([\d]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)N ([\d]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt:([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt: ([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt:([\d]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt: ([\d]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt ([\d,]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt([\d]+\.\d{0,2})") IS NOT NULL
        OR REGEXP_EXTRACT(message, r"(?i)Amt ([\d]+\.\d{0,2})") IS NOT NULL )
      AND NOT REGEXP_CONTAINS(LOWER(message), 'buy|paying|deposit|loan|website|possible|posi|posa|webi|postage|expose|suppose')
      AND (LOWER(message) LIKE '%pos%'
        OR LOWER(message) LIKE '%atm%'
        OR LOWER(message) LIKE '%web%')
    ORDER BY
      thread_id )
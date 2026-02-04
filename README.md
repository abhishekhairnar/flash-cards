/* OPEN TICKETS � partition-friendly, sargable on TK.PROCESS_DATE */

WITH tickets_base AS (
    SELECT
        TK.ID,
        TK.TN,
        TK.TITLE,
        TK.QUEUE_ID,
        TK.CUSTOMER_ID,
        TK.USER_ID,
        TK.TAG_ID,
        TK.TAG_ID_QUERY,
        TK.TAG_ID_SUB,
        TK.TICKET_STATE_ID,
        TK.CREATE_TIME,
        TK.CHANGE_TIME,
        TK.PROCESS_DATE,      -- closed date for closed tickets
        TK.REF_TICKET_ID,
        TK.COMMENTS,
        TK.TICKET_PRIORITY_ID
    FROM FDEMS_FDMS.TICKET TK
    /* Keep filters sargable & aligned with partitions */
    WHERE TK.PROCESS_DATE <> 19990101  -- exclude open
      /* Power BI Incremental Refresh will fold these: */
      /* TK.PROCESS_DATE >= :RangeStart AND TK.PROCESS_DATE < :RangeEnd */
)
, article_counts AS (
    /* Pre-aggregate only for relevant tickets via EXISTS */
    SELECT
        a.ticket_id,
        COUNT(CASE WHEN a.article_sender_type_id = 3 THEN 1 END) AS incoming_count,
        COUNT(CASE WHEN a.article_sender_type_id = 1
                    AND a.communication_channel_id <> 3 THEN 1 END) AS outgoing_count
    FROM FDEMS_FDMS.ARTICLE a
    WHERE a.article_sender_type_id IN (1, 3)
      AND EXISTS (SELECT 1 FROM tickets_base t WHERE t.id = a.ticket_id) 
      AND a.process_date <> 19990101
    GROUP BY a.ticket_id
)
SELECT
    tb.TN,
    tb.TITLE,
    tb.QUEUE_ID,
    q.NAME AS QUEUE_NAME,
    tb.CUSTOMER_ID,
    u.LOGIN AS USER_ID,
    TRIM(u.FIRST_NAME || ' ' || u.LAST_NAME) AS USER_NAME,
    tr.NAME AS UNIT,
    tb.TAG_ID,
    tb.TAG_ID_QUERY,
    SUBSTR(tq.NAME, INSTR(tq.NAME, '::') + 2) AS QUERY_TYPE,
    SUBSTR(tsq.NAME, INSTR(tsq.NAME, '::', -1) + 2) AS SUB_QUERY_TYPE,
    tb.TICKET_STATE_ID AS TICKET_STATE_ID,
    ts.NAME AS TICKET_STATUS,
    tst.NAME AS CASE_STATUS,
    tb.CREATE_TIME,
    tb.CHANGE_TIME,
    tb.PROCESS_DATE,

    /* Human-friendly rounded age for display (if needed) */
    ROUND( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24, 2 ) AS AGE_HOURS,

    /* Bucket on raw hours to avoid boundary misclassification */
    CASE
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 1  THEN '00-01 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 4  THEN '01-04 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 8  THEN '04-08 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 12 THEN '08-12 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 24 THEN '12-24 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 48 THEN '24-48 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 72 THEN '48-72 Hrs'
        ELSE '>72 Hrs'
    END AS AGING_BUCKET,

    tkref.TN AS REF_TN,
    tb.COMMENTS,
    tp.NAME AS PRIORITY,
    NVL(ac.incoming_count, 0) AS incoming_count,
    NVL(ac.outgoing_count, 0) AS outgoing_count
FROM tickets_base tb
JOIN FDEMS_FDMS.TICKET_STATE ts
  ON ts.ID = tb.TICKET_STATE_ID
JOIN FDEMS_FDMS.TICKET_STATE_TYPE tst
  ON ts.TYPE_ID = tst.ID
  AND tst.ID NOT IN (6, 7)
JOIN FDEMS_FDMS.QUEUE q
  ON q.ID = tb.QUEUE_ID
JOIN FDEMS_FDMS.USERS u
  ON u.ID = tb.USER_ID
LEFT JOIN FDEMS_FDMS.TAG tr
  ON tr.ID = tb.TAG_ID
LEFT JOIN FDEMS_FDMS.TAG tq
  ON tq.ID = tb.TAG_ID_QUERY
LEFT JOIN FDEMS_FDMS.TAG tsq
  ON tsq.ID = tb.TAG_ID_SUB
LEFT JOIN FDEMS_FDMS.TICKET tkref
  ON tkref.ID = tb.REF_TICKET_ID
LEFT JOIN FDEMS_FDMS.TICKET_PRIORITY tp
  ON tp.ID = tb.TICKET_PRIORITY_ID
LEFT JOIN article_counts ac
  ON ac.ticket_id = tb.ID;
  
  
/* CLOSED TICKETS � partition-friendly, sargable on TK.PROCESS_DATE */
WITH tickets_base AS (
    SELECT
        TK.ID,
        TK.TN,
        TK.TITLE,
        TK.QUEUE_ID,
        TK.CUSTOMER_ID,
        TK.USER_ID,
        TK.TAG_ID,
        TK.TAG_ID_QUERY,
        TK.TAG_ID_SUB,
        TK.TICKET_STATE_ID,
        TK.CREATE_TIME,
        TK.CHANGE_TIME,       -- last modified; equals closed datetime when closed
        TK.PROCESS_DATE,      -- = closed date for closed tickets
        TK.REF_TICKET_ID,
        TK.COMMENTS,
        TK.TICKET_PRIORITY_ID
    FROM FDEMS_FDMS.TICKET TK
    /* Keep predicates sargable & aligned with partitions */
    WHERE TK.PROCESS_DATE <> 19990101   -- exclude open
      AND TK.PROCESS_DATE >= :RangeStart
      AND TK.PROCESS_DATE <  :RangeEnd
)
, article_counts AS (
    /* Pre-aggregate only for relevant tickets via EXISTS */
    SELECT
        a.ticket_id,
        COUNT(CASE WHEN a.article_sender_type_id = 3 THEN 1 END) AS incoming_count,
        COUNT(CASE WHEN a.article_sender_type_id = 1
                    AND a.communication_channel_id <> 3 THEN 1 END) AS outgoing_count
    FROM FDEMS_FDMS.ARTICLE a
    WHERE a.article_sender_type_id IN (1, 3)
      AND EXISTS (SELECT 1 FROM tickets_base t WHERE t.id = a.ticket_id)
      AND a.process_date <> 19990101
      AND a.PROCESS_DATE >= :RangeStart
      AND a.PROCESS_DATE <  :RangeEnd
    GROUP BY a.ticket_id
)
SELECT
    tb.TN,
    tb.TITLE,
    tb.QUEUE_ID,
    q.NAME AS QUEUE_NAME,
    tb.CUSTOMER_ID,
    u.LOGIN AS USER_ID,
    TRIM(u.FIRST_NAME || ' ' || u.LAST_NAME) AS USER_NAME,
    tr.NAME AS UNIT,
    tb.TAG_ID,
    tb.TAG_ID_QUERY,
    SUBSTR(tq.NAME, INSTR(tq.NAME, '::') + 2) AS QUERY_TYPE,
    SUBSTR(tsq.NAME, INSTR(tsq.NAME, '::', -1) + 2) AS SUB_QUERY_TYPE,
    tb.TICKET_STATE_ID AS TICKET_STATE_ID,
    ts.NAME AS TICKET_STATUS,
    tst.NAME AS CASE_STATUS,
    tb.CREATE_TIME,
    tb.CHANGE_TIME,
    tb.PROCESS_DATE,

    /* Age to closure (hours) */
    ROUND( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24, 2 ) AS AGE_HOURS,

    /* Stable bucket on raw hours */
    CASE
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 1  THEN '00-01 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 4  THEN '01-04 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 8  THEN '04-08 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 12 THEN '08-12 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 24 THEN '12-24 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 48 THEN '24-48 Hrs'
        WHEN ( (tb.CHANGE_TIME - tb.CREATE_TIME) * 24 ) <= 72 THEN '48-72 Hrs'
        ELSE '>72 Hrs'
    END AS AGING_BUCKET,

    tkref.TN AS REF_TN,
    tb.COMMENTS,
    tp.NAME AS PRIORITY,
    NVL(ac.incoming_count, 0) AS incoming_count,
    NVL(ac.outgoing_count, 0) AS outgoing_count
FROM tickets_base tb
JOIN FDEMS_FDMS.TICKET_STATE ts
  ON ts.ID = tb.TICKET_STATE_ID
JOIN FDEMS_FDMS.TICKET_STATE_TYPE tst
  ON ts.TYPE_ID = tst.ID
  AND tst.ID NOT IN (6, 7)
JOIN FDEMS_FDMS.QUEUE q
  ON q.ID = tb.QUEUE_ID
JOIN FDEMS_FDMS.USERS u
  ON u.ID = tb.USER_ID
LEFT JOIN FDEMS_FDMS.TAG tr
  ON tr.ID = tb.TAG_ID
LEFT JOIN FDEMS_FDMS.TAG tq
  ON tq.ID = tb.TAG_ID_QUERY
LEFT JOIN FDEMS_FDMS.TAG tsq
  ON tsq.ID = tb.TAG_ID_SUB
LEFT JOIN FDEMS_FDMS.TICKET tkref
  ON tkref.ID = tb.REF_TICKET_ID
LEFT JOIN FDEMS_FDMS.TICKET_PRIORITY tp
  ON tp.ID = tb.TICKET_PRIORITY_ID
LEFT JOIN article_counts ac
  ON ac.ticket_id = tb.ID;

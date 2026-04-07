-- Event Log Query (ANSI SQL)

-- Fonte: Salesforce
SELECT
    CAST(ID_PROPOSTA AS VARCHAR)            AS Case_ID,
    CAST(AGENCIA AS VARCHAR)           AS Activity,
    CAST(DATA_INICIO AS TIMESTAMP)  AS Timestamp_Start,
    NULL                                       AS Timestamp_End,
    'Salesforce' AS Source
FROM crm_propostas
WHERE CAST(AGENCIA AS VARCHAR) IN ('BH-01', 'SP-01')
ORDER BY Timestamp_Start;

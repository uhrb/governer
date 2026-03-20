Execute following plan:

1. Take a look at governer.uharabtsou.customers table and find out columns which potentially can contain PII data, but not marked with tag named "pii".  To get information about tags, inspect column_tags table.
2. For each column you found on step 1, generate SQL statement to add tag "SET TAG ON COLUMN governer.uharabtsou.customers.<column_name> `pii`=`<suspected_pii_type>`", where suspected_pii_type - type of PII data you suspect.
3. For each column you found on step 1, express your confidence and provide number from 0.0 to 1.0, where 0.0 - you absolutely not confident and 1.0 - you 100% sure 
4. Generate answer for me in clear text but format it as  JSON. Do not include additional text, markdown or explanations, just results formatted as JSON. Answer should include: column_name, suspected_pii_type, confidence, sql_statement.
import psycopg2

conn = psycopg2.connect(
    database='wholesale',
    user='root',
    sslmode='verify-full',
    sslrootcert='../certs/ca.crt',
    sslcert='../certs/client.root.crt',
    sslkey='../certs/client.root.key',
    port=26278,
    host='xcnd45.comp.nus.edu.sg',
    password='cs4224hadmin'
)
import snowflake.connector # To conect pyrhon with snowflake
import pandas as pd        # To provide a DataFrame and put fetching data into it
import calendar            # To use month_name method to extract month from datetime field


try:
    # create Snowflake connection(conn)
    conn = snowflake.connector.connect(
    user="ahmadahmadi",
    password="**********",
    account="XSLRVBS-CO30522",
    database="WILDWEST",
    schema="PROCESSED"
)

    # Create a cursor object(cur)
    cur = conn.cursor()

    # SQL query
    sql_query2 = '''
WITH MSGDETAIL AS
( 
    SELECT * FROM RMSG9906
        UNION ALL
    SELECT * FROM BMSG0001
        UNION ALL
    SELECT * FROM BMSG9901
        UNION ALL
    SELECT * FROM BMSG9902    
        UNION ALL
    SELECT * FROM BMSG9903
        UNION ALL
    SELECT * FROM BMSG9904
)
SELECT 
    * , 
    case substr(con_date, 1,2)
            when '00' then monthname(to_date('20' || con_date, 'yyyymmdd'))
            when '99' then monthname(to_date('19' || con_date, 'yyyymmdd'))
    end as month,
    case substr(con_date, 1,2)
            when '00' then to_char(year(to_date('20' || con_date, 'yyyymmdd')))
            when '99' then to_char(year(to_date('19' || con_date, 'yyyymmdd')))
    end as year
FROM MSGDETAIL
WHERE con_date IS NOT NULL AND 
    coalesce(trim(term_st), '') <> ''
ORDER BY term_st ASC,year, month;
        
    '''

    # Fetching all of needs frome snowflake and put into a pandas dataframe(result_df)
    cur.execute(sql_query2)
    result_df = cur.fetch_pandas_all()
    #Transform and clean data
    # Convert 'CON_DATE' to string in order to change it fromat 'yymmdd' to 'yyyy,mm,dd' and extract year, month, day from it
    df['CON_DATE'] = df['CON_DATE'].astype(str)     # Ofdourse I do it in SQL query but I put this code by python here

    # Extract year(using list comprihention to make year for 4 digit format), month, and day 
    df['Year'] = df['CON_DATE'].str[-4:].apply(lambda x: f"20{x}" if x == '00' else f"19{x}")
    df['Month'] = df['CON_DATE'].str[-6:-4].astype(int)
    df['Day'] = df['CON_DATE'].str[:-6].astype(int)

    # Convert to datetime and save on CON_DATE again
    df['CON_DATE'] = pd.to_datetime(df[['Year', 'Month', 'Day']], errors='coerce')

    # Extract month name
    df['Month_Name'] = df['Month'].apply(lambda x: calendar.month_name[x])
    result_df.to_csv("total.csv", index=False) # I don't have any permision to transform or create any extra object on snowflake so I have to save transformed data       to csv file and work on it     


finally:
    # Close the connection at the end if connection still open
    if conn:
        conn.close()

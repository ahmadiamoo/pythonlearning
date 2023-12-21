
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt


try:
    # create Snowflake connection(conn)
    conn = snowflake.connector.connect(
    user="ahmadahmadi",
    password="Ef7ciscoccnp",
    account="XSLRVBS-CO30522",
    database="WILDWEST",
    schema="PROCESSED"
)

    # Create a cursor object(cur)
    cur = conn.cursor()

    # SQL query to count and sum based on the provided logic
    sql_query1 = '''
        SELECT 
            COUNT(rev_amt) AS total_Domestic_message,
            ROUND(SUM(rev_amt)) AS total_revenue
        FROM (
            SELECT * FROM RMSG9906    UNION ALL
            SELECT * FROM BMSG0001    UNION ALL
            SELECT * FROM BMSG9901    UNION ALL
            SELECT * FROM BMSG9902    UNION ALL 
            SELECT * FROM BMSG9903    UNION ALL
            SELECT * FROM BMSG9904
        ) AS MSGDETAIL 
        RIGHT JOIN (
            SELECT area_code FROM rescust
            UNION ALL 
            SELECT area_code FROM buscust
        ) AS localcustomer
        ON MSGDETAIL.orig_area_code = localcustomer.area_code
        WHERE con_date IS NOT NULL AND ASCII(term_st) <> 32
    '''
    sql_query2 = '''
        SELECT 
            term_st as state, 
            COUNT(rev_amt) AS total_message_per_state,
            sum(rev_amt)as total_revenue_per_state
        FROM (
            SELECT * FROM RMSG9906     UNION ALL
            SELECT * FROM BMSG0001    UNION ALL
            SELECT * FROM BMSG9901    UNION ALL
            SELECT * FROM BMSG9902    UNION ALL 
            SELECT * FROM BMSG9903    UNION ALL
            SELECT * FROM BMSG9904
            ) AS MSGDETAIL 
        RIGHT JOIN (
            SELECT area_code FROM rescust
        UNION ALL 
            SELECT area_code FROM buscust
                    ) AS localcustomer
        ON MSGDETAIL.orig_area_code = localcustomer.area_code
            WHERE con_date IS NOT NULL  AND ASCII(term_st) <> 32 
        GROUP BY term_st
        ORDER BY term_st 
    '''
    # Fetching all of needs frome snowflake and put into a pandas dataframe(result_df)
    cur.execute(sql_query2)
    result_df = cur.fetch_pandas_all()
    #result_df.to_csv("data/totalmsg&rev.csv", index=False)

    # Print the result DataFrame
    #print(result_df)
    
    plt.figure(figsize=(20, 20))
    ax = result_df.plot(kind='bar', x='STATE', y='TOTAL_MESSAGE_PER_STATE',edgecolor='brown', alpha=0.7, legend=False)
    plt.title('Total Messages per State')
    plt.xlabel('STATE', rotation=45, ha='right')
    plt.xlim(-0.5, len(result_df) - 0.5)
    ax.set_xticklabels(result_df['STATE'], rotation=45, ha='right')  # 'rotation' specifies the rotation angle, 'ha' specifies the alignment

    plt.ylabel('Total Messages')
    plt.show()

    

finally:
    # Close the connection at the end 
    if conn:
        conn.close()

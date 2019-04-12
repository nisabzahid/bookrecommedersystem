import psycopg2
try:
    connection = psycopg2.connect(user = "blue",
                                  password = "blue",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "db")
    cursor = connection.cursor()

    sql = """CREATE TABLE IF NOT EXISTS REVIEWS
                          (
                            ID SERIAL   NOT NULL PRIMARY KEY,
                            TITLE text, 
                            POINTS integer ,
                            DESCRIPTION text,
                            TASTER_NAME text,
                            TASTER_TWITTER_HANDLE text,
                            PRICE text,
                            DESIGNATION text,
                            VARIETY text,
                            REGION_1 text,
                            REGION_2 text,
                            PROVINCE text,
                            COUNTRY text,
                            WINERY text 
                          );
      """

    cursor.execute(sql)
    connection.commit()
    print("review table has been  created.")
  
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
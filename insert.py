import psycopg2
import json

try:
    connection = psycopg2.connect(user = "blue",
                                  password = "blue",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "db")
    cursor = connection.cursor()
    cursor.execute("GRANT ALL ON ALL TABLES IN SCHEMA public TO blue;")

    with open("raw_data.json",encoding="utf8") as json_data:
        li=json.load(json_data)
       
    #print(type(x))== list
    

        for l in li: 
               #type(l)== dictionary    
            points=l["points"]
            title=l["title"]
            description=l["description"]
            taster_name=l["taster_name"]
            taster_twitter_handle=l["taster_twitter_handle"]
            price=l["price"]
            designation=l["designation"]
            variety=l["variety"]
            region_1=l["region_1"]
            region_2=l["region_2"]
            province=l["province"]
            country=l["country"]
            winery=l["winery"]        
            sql = """INSERT INTO REVIEWS
                                  (          
                                    POINTS ,
                                    TITLE,
                                    DESCRIPTION ,
                                    TASTER_NAME ,
                                    TASTER_TWITTER_HANDLE ,
                                    PRICE ,
                                    DESIGNATION ,
                                    VARIETY ,
                                    REGION_1,
                                    REGION_2,
                                    PROVINCE,
                                    COUNTRY,
                                    WINERY
                                  )

                            values(      
                                   {},{},{},{},{},{},{},{},{},{},{},{},{}
                                  );
                      """.format(points,title,description,taster_name,taster_twitter_handle,price,designation, variety,region_1,region_2,province,country,winery)
            
            
           # print(sql)      
            cursor.execute(sql)
            connection.commit()
    print(" json file to table insertion done.   ")
    
except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

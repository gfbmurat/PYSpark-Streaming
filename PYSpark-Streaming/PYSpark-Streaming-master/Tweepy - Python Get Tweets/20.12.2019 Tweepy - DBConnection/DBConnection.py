import psycopg2


# Tweeti doğrudan json şeklinde kaydeder(TWEETS TABLE)
def insert_to_tweet(tweet):
    con = psycopg2.connect(
        host="localhost", database="Twitter", user="postgres", password="123", port=5432)
    cur = con.cursor()
    cur.execute("insert into tweets (tweet) values('{}')".format(tweet))
    cur.close()
    con.commit()
    con.close()


# location adedini bir arttırır.
def update_count_location(location):
    con = psycopg2.connect(host="localhost", database="Twitter", user="postgres", password="123", port=5432)
    cur = con.cursor()
    cur.execute("update location set count=count+1 where city=('{}')".format(location))
    cur.close()
    con.commit()
    con.close()


# location ismine göre location bilgilerini dönderir..
def find_location(location):
    con = psycopg2.connect(host="localhost", database="Twitter", user="postgres", password="123", port=5432)
    cur = con.cursor()
    cur.execute("select id,city,count from location where city=('{}')".format(location))
    row = cur.fetchall()
    # print(row)
    if len(row) == 0:
        print("SEHIR BULUNAMADI!")
    else:
        print("Şehir id'si:{0}\nŞehir ad:{1}\nŞehir adet:{2}".format(row[0][0], row[0][1], row[0][2]))


# location ismine göre böyle bir şehir varsa count 1 yoksa count 0 dönderecektir.
def location_control(location):
    con = psycopg2.connect(host="localhost", database="Twitter", user="postgres", password="123", port=5432)
    cur = con.cursor()
    cur.execute("select count(id) from location where city like ('%{}%')".format(location))
    row = cur.fetchall()
    print(row[0][0])
    if row[0][0] == 0:
        print("Böyle bir şehir yok")
    else:
        update_count_location(location)
    cur.close()
    con.commit()
    con.close()

import psycopg2


def insert_to_tweet(tweet):
    con = psycopg2.connect(
        host="localhost", database="Twitter", user="postgres", password="123", port=5432)
    cur = con.cursor()
    cur.execute("insert into tweets (tweet) values('{}')".format(tweet))
    cur.close()
    con.commit()
    con.close()

import tweepy
import json
import DBConnection as db

API_KEY = ""
API_SECRET_KEY = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""
auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


class MaxListener(tweepy.StreamListener):

    def __init__(self):
        print("Tweepy Live Streaming Starts.")

    def on_data(self, raw_data):
        self.process_data(raw_data)
        return True

    def process_data(self, raw_data):
        raw_data = raw_data.replace("'", " ")
        print(raw_data)
        location = self.location_draw(raw_data)
        if location is None:
            print("Bu tweet database'e kaydedilmedi(Location is None)")
        else:
            db.insert_to_tweet(raw_data)
            db.location_control(location)

    def location_draw(self, raw_data):
        json_format = json.loads(raw_data)  # Veri türü str convert dict{}
        location = json_format['user']['location']
        if location is None:
            print("Location is None")
        else:
            location = location.split(",")[0]  # Malatya Türkiye gibi bir değer varsa sadece Malatyayı alır.
            location = location.upper()
        print(location)
        return location

    def save_txt(self, tweets):
        with open('deprem1.txt', 'a', encoding="utf-8") as txt_file:
            txt_file.write(tweets)

    def save_location(self, location):
        if location is None:  # Eğer location yok ise(Type Error Hatası vermemesi icin)
            print("Location Belirtilmedi")
        else:
            with open('location.txt', 'a', encoding="utf-8") as txt_file:
                txt_file.write(location + "\n")

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    # Create Stream


class MaxStream:
    def __init__(self, auth, listener):
        self.stream = tweepy.Stream(auth=auth, listener=listener)

    def start(self, keyword_list):
        self.stream.filter(track=keyword_list)


if __name__ == "__main__":
    listener = MaxListener()
    # api = tweepy.API(auth)
    stream = MaxStream(auth, listener)
    stream.start(["#deprem"])

import json
import sqlite3
import string
#from translate import Translator

filename_json = 'three_minutes_tweets.json.txt' #json filename
filename_afinn = 'AFINN-111.txt' #afinn filename

#create connection to db
db = sqlite3.connect('twitterDataBase.db')
cur = db.cursor()

#1st step - create table twitter_raw_data
def createRawDataTable():
    cur.execute ('create table if not exists twitter_raw_data'
                 '(name varchar(50)'
                 ', tweet_text varchar(150)'
                 ', country_code varchar(5)'
                 ', display_url varchar(50)'
                 ', lang varchar(6)'
                 ', created_at varchar(20)'
                 ', location varchar(45))')
    db.commit()

def dropTables():
    cur.execute('drop table twitter_raw_data')
    cur.execute('drop table twitter_users')
    cur.execute('drop table twitter_languages')
    cur.execute('drop table twitter_countries')
    cur.execute('drop table twitter_data')
    db.commit()

#2nd step - load three_minutes_tweets into table
def insertRawData(filename):
    """
    :param filename: -> имя json для парсинга
    :return: -> заполняет данные в target table [twitter_raw_data]
    """
    with open(filename) as file:
        counter = 0
        for tweet in file:
            tweet = json.loads(tweet)
            if 'delete' in tweet:
                del tweet['delete']
            elif 'user' in tweet:
                name = tweet['user']['name'],
                tweet_text = tweet['text'],
                country_code = tweet['place']['country_code'] if tweet['place'] else 'null',
                display_url = f'https://twitter.com/statuses/{tweet["id_str"]}',
                lang = tweet['lang'],
                created_at = tweet['created_at'],
                location = tweet['user']['location']

                cur.execute("insert into twitter_raw_data"
                            "(name, tweet_text, country_code, display_url, lang, created_at, location) "
                            "values (?, ?, ?, ?, ?, ?, ?);",
                            (str(name), str(tweet_text), str(country_code), str(display_url), str(lang), str(created_at), str(location)))
                counter += 1
        db.commit()

#3rd step - add tweet_sentiment_column
def addSentimentColumn():
    cur.execute ('alter table twitter_raw_data '
                 'add column tweet_sentiment integer(10) default 0')
    db.commit()

#4th step - create normolized tables
def createNormalizedTables():
    cur.execute('create table if not exists twitter_users '
                '(name varchar(50)'
                ', location varchar(45))')

    cur.execute('create table if not exists twitter_languages '
                '(language varchar(6))')

    cur.execute('create table if not exists twitter_countries '
                '(country varchar(5))')

    cur.execute('create table if not exists twitter_data '
                 '(name varchar(50)'
                 ', tweet_text varchar(150)'
                 ', country_code varchar(5)'
                 ', display_url varchar(50)'
                 ', lang varchar(6)'
                 ', created_at varchar(20)'
                 ', location varchar(45)'
                 ', tweet_sentiment integer(10) default 0'
                 ', foreign key (name) references "twitter_users"("name")'
                 ', foreign key (lang) references "twitter_languages"("language")'
                 ', foreign key (country_code) references "twitter_countries"("country"))')
    db.commit()

def fillUsersTable():
    statement = cur.execute('select name, location from twitter_raw_data').fetchall()
    for i, user in enumerate(statement, 1):
        cur.execute('insert into twitter_users'
                    '(name, location)'
                    ' values (?,?);',
                    (user[0], user[1]))
    db.commit()

def fillLanguagesTable():
    statement = cur.execute('select distinct lang from twitter_raw_data').fetchall()
    for i, lang in enumerate(statement, 1):
        cur.execute('insert into twitter_languages'
                    '(language)'
                    ' values (?);',
                    (lang))
    db.commit()

def fillCounriesTable():
    statement = cur.execute('select distinct country_code from twitter_raw_data').fetchall()
    for i, country in enumerate(statement, 1):
        cur.execute('insert into twitter_countries'
                    '(country)'
                    ' values (?);',
                    (country))
    db.commit()

def createSentimentDict(filename):
    """
    :param filename: -> файл слово-окраска
    :return: hash table -> dict
    """
    sentiment_dict = {}
    with open(filename) as file:
        lines = file.readlines()
        for line in lines:
            split = line.split()
            sentiment = ' '.join(split[:-1])
            value = split[-1]
            sentiment_dict[sentiment] = value
    return sentiment_dict



def calculateTweetSentiment(phrase):
    """
    Фунция для расчёта эмоциональной окраски твита.
    Так же убираем все знаки, кроме букв -> https://stackoverflow.com/questions/265960/best-way-to-strip-punctuation-from-a-string-in-python
    :param phrase: -> text
    :param dictionary: -> sentiment_dict
    :return:  -> integer value
    """
    #trans = Translator(from_lang='br', to_lang="en") #нельзя переводить с английского на английски :( . https://pypi.org/project/translate/
    sentiment = 0
    dictionary = createSentimentDict(filename_afinn)
    norm = str.maketrans('', '', string.punctuation)
    # try:
    #     tweet = trans.translate(phrase.translate(norm)).lower().split()
    # except Exception as e:  #игнорируем ошибки.  В основном -  перевода, если у нас нет доступа к интернету.
    #     tweet = phrase.translate(norm).lower().split()
    tweet = phrase.translate(norm).lower().split()
    for word in tweet:
        if word in dictionary:
            sentiment += int(dictionary[word])
    return sentiment

def fillTargetTable():
    sentiment_dict =  createSentimentDict(filename_afinn)
    statement = cur.execute('select name, tweet_text, country_code, display_url, lang, created_at, location from twitter_raw_data').fetchall()
    for i, tweet in enumerate(statement, 1):
        cur.execute('insert into twitter_data'
                     '(name, tweet_text, country_code, display_url, lang, created_at, location, tweet_sentiment)'
                     'values (?,?,?,?,?,?,?,?);',
                    (str(tweet[0]), str(tweet[1]), str(tweet[2]), str(tweet[3]), str(tweet[4]), str(tweet[5]),str(tweet[6]), calculateTweetSentiment(tweet[1])))
    db.commit()

def printStatistics():
    #наиболее и наименее счастливую страну, локацию и пользователя (дял пользователя - вместе с его твитами)

    happiestUser = cur.execute('select name, tweet_text '
                               'from twitter_data '
                               'where tweet_sentiment = '
                               '(select max(tweet_sentiment) '
                               'from twitter_data)').fetchone()
    unHappiestUser = cur.execute('select name, tweet_text '
                                 'from twitter_data '
                                 'where tweet_sentiment = '
                                 '(select min(tweet_sentiment) '
                                 'from twitter_data)').fetchone()

    val = cur.execute('select sum(tweet_sentiment) from twitter_data group by lang').fetchall()
    maxL = int(max(val)[0])
    minL = int(min(val)[0])

    happiestCountry = cur.execute('select lang, sum(tweet_sentiment) '
                                  'from twitter_data '
                                  'group by lang '
                                  'having sum(tweet_sentiment) >= {value};'.format(value=maxL)).fetchall()
    unHappiestCountry = cur.execute('select lang, sum(tweet_sentiment) '
                                  'from twitter_data '
                                  'group by lang '
                                  'having sum(tweet_sentiment) >= {value};'.format(value=minL)).fetchall()


    happiestLocation = cur.execute('select location from (select location, sum(tweet_sentiment) from twitter_data where location <> "" group by location order by sum(tweet_sentiment) desc) limit 1').fetchone()

    unHappiestLocation = cur.execute('select location from (select location, sum(tweet_sentiment) from twitter_data where location <> "" group by location order by sum(tweet_sentiment) asc) limit 1').fetchone()

    print()
    print(f'Happiest country: {happiestCountry[0]}')
    print(f'Unhappiest country: {unHappiestCountry[0]}')
    print()
    print(f'Happiest location: {happiestLocation[0]}')
    print(f'Unhappiest location: {unHappiestLocation[0]}')
    print()
    print(f'Happiest user: {happiestUser[0], happiestUser[1]}')
    print(f'Unhappiest user: {unHappiestUser[0], unHappiestUser[1]}')
    print()


if __name__ == '__main__':
    #dropTables() #for test
    createRawDataTable()
    insertRawData(filename_json)
    addSentimentColumn()
    createNormalizedTables()
    fillUsersTable()
    fillLanguagesTable()
    fillCounriesTable()
    createSentimentDict(filename_afinn)
    fillTargetTable()
    printStatistics()
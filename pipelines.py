# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import w3lib.html
import json
from datetime import datetime
from scrapy import signals
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from transformers import pipeline
from sklearn.metrics import accuracy_score
import cx_Oracle

class EsgNewscrawlerPipeline:
    def process_item(self, item, spider):
        return item
    
class DataCleaningPipeline:
    def process_item(self,item,spider):
        SpecialCharacters = "[!@#$%^&*()[]{};:,./<>?\|`~-=_+–-|]"
        date_pattern = r'\d+\s+[A-Za-z]+\s+\d+'
        indonesian_to_english = {
            'senin': 'monday',
            'selasa': 'tuesday',
            'rabu': 'wednesday',
            'kamis': 'thursday',
            'jumat': 'friday',
            'sabtu': 'saturday',
            'minggu': 'sunday',
            'januari': 'january',
            'februari': 'february',
            'maret': 'march',
            'april': 'april',
            'mei': 'may',
            'juni': 'june',
            'juli': 'july',
            'agustus': 'august',
            'september': 'september',
            'oktober': 'october',
            'november': 'november',
            'desember': 'december',
            'jan' : 'jan',
            'feb' : 'feb',
            'mar' : 'mar',
            'apr' : 'apr',
            'jun' : 'jun',
            'jul' : 'jul',
            'agu' : 'aug',
            'sep' : 'sep',
            'okt' : 'oct',
            'nov' : 'nov',
            'des' : 'dec',
            '/01/' : ' jan ',
            '/02/' : ' feb ',
            '/03/' : ' mar ',
            '/04/' : ' apr ',
            '/05/' : ' may ',
            '/06/' : ' jun ',
            '/07/' : ' jul ',
            '/08/' : ' aug ',
            '/09/' : ' sep ',
            '/10/' : ' oct ',
            '/11/' : ' nov ',
            '/12/' : ' dec ',
            '.01.' : ' jan ',
            '.02.' : ' feb ',
            '.03.' : ' mar ',
            '.04.' : ' apr ',
            '.05.' : ' may ',
            '.06.' : ' jun ',
            '.07.' : ' jul ',
            '.08.' : ' aug ',
            '.09.' : ' sep ',
            '.10.' : ' oct ',
            '.11.' : ' nov ',
            '.12.' : ' dec '
        }

        if 'Title' in item:
            removeSpecialChars = re.sub(SpecialCharacters, " ", item['Title']).strip()
            item['Title'] = ' '.join(removeSpecialChars.split())
            

        # Clean and remove HTML tags from the 'Content' field
        if 'Content' in item:
            removeSpecialChars = re.sub(SpecialCharacters, " ", item['Content']).strip()
            removeHtmlTags = w3lib.html.remove_tags(removeSpecialChars).strip()
            removeHtmlTags = removeHtmlTags.replace('\"','')
            removeHtmlTags = removeHtmlTags.replace('\n','')
            removeHtmlTags = removeHtmlTags.replace('\t','')
            cleaned_content = ' '.join(removeHtmlTags.split())
            item['Content'] = cleaned_content
            
        
        # Clean and reformat the 'Publish date' field
        if 'Publish date' in item:
            format_tgl = [
            "%A, %d %B %Y - %I:%M %p","%A, %d %B %Y - %I:%M %p","%a, %d %B %Y - %I:%M %p","%a, %d %B %y - %I:%M %p","%a, %B %d %y - %I:%M %p","%a, %B %d %Y - %I:%M %p",
            "%A, %d %b %Y - %I:%M %p","%A, %d %b %Y - %I:%M %p","%a, %d %b %Y - %I:%M %p","%a, %d %b %y - %I:%M %p","%a, %b %d %y - %I:%M %p","%a, %b %d %Y - %I:%M %p",
            "%A %d %B %Y %H:%M","%a %d %B %Y %H:%M","%d %B %Y %H:%M","%d %b %Y %H:%M","%B %d %Y %H:%M","%b %d %Y %H:%M","%d %b %y %H:%M","%b %d %Y %I:%M %p",
            "%A %d %b %Y %H:%M","%a %d %b %Y %H:%M","%b %d %Y","%B %d %Y","%d %b %Y","%d %B %Y","%d %b %y","%d %B %y","%B %d %Y %I:%M %p","%b %d %Y %I:%M %p",
            "%A %d %B %Y","%B %d %Y %I:%M %p","%A %d %B %Y","%A %d %b %Y","%A %d %b %Y - %H:%M","%a %d %b %Y - %H:%M","%a %b %d %Y","%a %d %b %Y – %H:%M",
            "%A %d %B %Y – %H:%M","%a %B %d %Y"
            ]
            parsed_date = None

            strdatetime = item['Publish date'].strip()
            print(item['Publish date'].strip())
            strcheck = isinstance(strdatetime,str)
            print("Is string ?" + str(strcheck))
            for indonesian, english in indonesian_to_english.items():
                strdatetime = strdatetime.replace(indonesian.capitalize() , english)
            match = re.search(date_pattern, strdatetime)
            strdatetime = match.group()
            print(strdatetime)
            for format_string in format_tgl:
                try:
                    parsed_date = datetime.strptime(strdatetime.strip(), format_string)
                    break  
                except ValueError:
                    pass
            if parsed_date:
                item['Publish date'] = parsed_date.strftime("%d-%m-%Y")
            else:
                print("Date parse format not found")

        
        # Clean the 'Topic' field
        if 'Topic' in item:
            item['Topic'] = item['Topic'].strip() 
    
        if 'SubTopic' in item:
            item['SubTopic'] = item['SubTopic'].strip()
        
        if 'Keyword' in item:
            item['Keyword'] = item['Keyword'].strip()
        return item

class UrlValidationPipeline:
    def __init__(self, oracle_user, oracle_pwd, oracle_dsn, oracle_table):
        self.oracle_user = oracle_user
        self.oracle_pwd = oracle_pwd
        self.oracle_dsn = oracle_dsn
        self.oracle_table = oracle_table
        self.unique_links = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            oracle_user=crawler.settings.get('ORACLE_USER'),
            oracle_pwd=crawler.settings.get('ORACLE_PWD'),
            oracle_dsn=crawler.settings.get('ORACLE_DSN'),
            oracle_table= "APP_IEM_INOVASI.IEM_TBL_NEWS_DETAIL_PICC"
        )

    def open_spider(self, spider):
        self.conn = cx_Oracle.connect(self.oracle_user, self.oracle_pwd, self.oracle_dsn)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        link = item['Link']

        if not self.is_unique_link(link) and not self.is_link_in_database(link):
           raise DropItem("Duplicate link: {}".format(link))

        return item

    def is_unique_link(self, link):
        if link in self.unique_links:
            return False
        else:
            self.unique_links.add(link)
            return True

    def is_link_in_database(self, link):
        # Query the database to check if the link already exists
        sql = f"""SELECT COUNT(*) FROM {self.oracle_table} WHERE NEWS_URL = :link"""
        self.cursor.execute(sql, link=link)
        result = self.cursor.fetchone()[0]

        return result > 0

    def close_spider(self, spider):
        self.conn.close()


class SentimentAnalysisPipeline:
    def __init__(self):
        self.sentiment_analyzer = pipeline(
            "zero-shot-classification",
            model="MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli-ling-wanli",
            device=0,
            max_length=512,
            truncation=True,
        )

    def process_item(self, item, spider):
        content = item['Content']

        # Perform sentiment analysis
        candidate_sentiments = ["positive", "neutral", "negative"]
        sentiment = self.sentiment_analyzer(
            content, candidate_sentiments, multi_label=False
        ).get('labels')[0]

        # Emit the custom signal to send the sentiment value to the spider
        spider.crawler.signals.send_catch_log(
            signal=signals.spider_opened,
            sender=spider,
            sentiment=sentiment,  # Send the sentiment value to the spider
        )

        item['Sentiment'] = sentiment
        return item

class SentimentAnalysisWithW11woPipeline:
    def process_item(self, item, spider):
        # Extract content from the item
        content = item['Content']
        
        # Update the item with the sentiment result
        pretrained_name = "w11wo/indonesian-roberta-base-sentiment-classifier"

        nlp = pipeline("sentiment-analysis",
        model=pretrained_name,
        tokenizer=pretrained_name, 
        max_length=512, 
        truncation=True
        )

        # Perform sentiment analysis
        sentiment = nlp(content)[0]['label']
        
        item['Sentiment'] = sentiment
        return item
    
class OracleInsertTablePipeline:
    def __init__(self, oracle_user, oracle_pwd, oracle_dsn, oracle_table):
        self.oracle_user = oracle_user
        self.oracle_pwd = oracle_pwd
        self.oracle_dsn = oracle_dsn
        self.oracle_table = oracle_table
        self.conn = None
        self.cursor = None
        self.existing_urls = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            oracle_user=crawler.settings.get('ORACLE_USER'),
            oracle_pwd=crawler.settings.get('ORACLE_PWD'),
            oracle_dsn=crawler.settings.get('ORACLE_DSN'),
            oracle_table= "APP_IEM_INOVASI.IEM_TBL_NEWS_DETAIL_PICC",
        )

    def open_spider(self, spider):
        self.conn = cx_Oracle.connect(self.oracle_user, self.oracle_pwd, self.oracle_dsn)
        self.cursor = self.conn.cursor()
        # Retrieve existing urls from the database
        self.existing_urls = self.get_existing_url()

    def close_spider(self, spider):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def process_item(self, item, spider):
        url = item['Link']

        if url in self.existing_urls:
            raise DropItem("Duplicate link: {}".format(url))
        str_date = item['Publish date']
        publish_date = datetime.strptime(str_date,'%d-%m-%Y')
        cursor = self.conn.cursor()
        try:
            sql = f"""INSERT INTO {self.oracle_table} (NEWS_URL, NEWS_CATEGORY,SUBCATEGORY, SENTIMENT, KEYWORD, NEWS, NEWS_DATE) 
                      VALUES (:NEWS_URL,:NEWS_CATEGORY,:SUBCATEGORY,:SENTIMENT,:KEYWORD,:NEWS,:NEWS_DATE)"""
            data =  {
                    'NEWS_URL':item['Link'], 
                    'NEWS_CATEGORY':item['Topic'], 
                    'SUBCATEGORY':item['SubTopic'], 
                    'SENTIMENT':item['Sentiment'], 
                    'NEWS_DATE':publish_date ,
                    'KEYWORD':item['Keyword'] ,
                    'NEWS':item['Content']
                    }
            cursor.execute(sql, data)
            self.conn.commit()
        except Exception as e:
            print("Error:", e)
            self.conn.rollback()
        finally:
            cursor.close()

        self.existing_urls.add(url)
        return item
    
    def get_existing_url(self):
        # Query the database to retrieve existing links
        existing_urls = set()
        sql = f"SELECT NEWS_URL FROM {self.oracle_table}"
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()

        for row in rows:
            existing_urls.add(row[0])

        return existing_urls
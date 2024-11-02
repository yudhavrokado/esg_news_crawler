import scrapy
import cx_Oracle
import json
import w3lib.html
from urllib.parse import urlparse
from datetime import datetime
from scrapy import signals

class NewspageSpider(scrapy.Spider):
    name = "NewsPage"
    
    def __init__(self, *args, **kwargs):
        super(NewspageSpider, self).__init__(*args, **kwargs)
        self.dt_crawl = datetime.now().strftime("%d-%m-%Y %H:%M")

    def start_requests(self):
        with open('searchengine.json') as se:
            s = json.load(se)
            for values in s:
                starturl = values["start_url"]
                with open('keyword.json') as data:
                    x = json.load(data)
                for item in x:
                    topic = item["Topic"]
                    subtopic = item["Sub Topic"]
                    keywords = item["Keyword"]
                    i=0
                    # Process each keyword
                    for keyword in keywords:
                        query = keyword 
                        print(query)
                        urls = [ f"{starturl}?q={query+' Pertamina'}%22&first={x}".format(x=x) for x in range(1, 50)]
                        print(urls)

                        for url in urls:
                            yield scrapy.Request(
                                url=url, 
                                callback=self.get_link, 
                                meta={'topic': topic, 
                                      'subtopic': subtopic, 
                                      'keyword': keyword})

    def get_link(self, response):
        links = response.xpath('//div/a/@href').extract()
        topic = response.meta.get('topic')
        subtopic = response.meta.get('subtopic')
        keyword = response.meta.get('keyword')
        
        for link in links:
            if 'https://' in link:
                yield scrapy.Request(
                    url=link, 
                    callback=self.get_news, 
                    meta={'tautan': link, 
                          'topic': topic, 
                          'subtopic': subtopic, 
                          'keyword': keyword
                          }
                    )

    def get_news(self, response):
        urls = response.meta.get('tautan')
        topik = response.meta.get('topic')
        subtopik = response.meta.get('subtopic')
        keyword = response.meta.get('keyword')
        parse = urlparse(urls).netloc
        domain = parse.replace("www.", "")

        with open('library.json') as jsondata:
            data = json.load(jsondata)

        for val in data:
            if val['domain'] == domain:
                judul = val['judul']
                konten = val['konten']
                konten1 = val['konten1']
                tgl = val['tgl']
                if judul == "":
                    continue

        try:
            title = response.xpath(judul).extract()
            if len(response.xpath(konten).extract()):
                contents = response.xpath(konten).extract()
            else:
                contents = response.xpath(konten1).extract()
            
            publish = response.xpath(tgl).extract()

            yield {
                'Link': urls,
                'Title': ' '.join(title).strip(),
                'Content': w3lib.html.remove_tags(' '.join(contents).strip()),
                'Publish date': ' '.join(publish).strip(),
                'Crawler date': self.dt_crawl,
                'Topic': topik,
                'SubTopic': subtopik,
                'Keyword': keyword,
                'Sentiment': '',
            }

        except UnboundLocalError:
            pass

        except ValueError:
            pass

        except Exception as e:
            print(f"Exception: {e}")

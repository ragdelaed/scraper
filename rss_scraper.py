#!/usr/bin/python

import MySQLdb
import feedparser
import urllib2
import tldextract

from time import mktime
from datetime import datetime

# get connection and cursor upfront, before entering loop
db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="P@ssw0rd",  # your password
                     db="scraper",        # name of the data base
                     use_unicode=True, 
                     charset="utf8")

cur = db.cursor()

cur.execute(
    "SELECT rss_feed_url FROM rss_feed_urls"
)
rss_feed_urls=cur.fetchall()

for i in rss_feed_urls:
    myfeed = feedparser.parse(i[0])
    for item in myfeed['items']:
        title = item.title
        link = item.link
        description = item.description
        source=i[0]
        extract=tldextract.extract(source)
        tld=extract.registered_domain
        try:
            dt = mktime(item.updated_parsed)
        except:
            import time
            dt=time.time()
        #print link
        #print source
       
        cur.execute(
            "SELECT title, COUNT(*) FROM rss_feed WHERE title = %s GROUP BY title",
            (title,)
        )
        # gets the number of rows affected by the command executed
        row_count = cur.rowcount
        if row_count == 0:
            try: 
                import time
                import os
                time=time.time()
                cwd = os.getcwd()
                file= cwd + "/tmp/" + tld + "_" + str(time)
                
                cur.execute("INSERT INTO rss_feed (title, link, description, updated, source, timestamp, disk_name) VALUES (%s,%s,%s,%s,%s,%s,%s)", (title, link, description, dt, source, time, file))
                
                print "working on '" + title + "' from '" + link + "'"
                from bs4 import BeautifulSoup
                from urllib import urlopen
                html = urlopen(link).read()
                soup=BeautifulSoup(html, "lxml")
                for script in soup(["script","style"]):
                    script.extract()
                text=soup.get_text()
                lines=(line.strip() for line in  text.splitlines())
                chunks=(phrase.strip() for line in lines for phrase in line.split(" "))
                text =  '\n'.join( chunk for chunk in chunks if chunk)

                try:
                    f=open(file,'w')
                    f.write(text.encode('utf8'))
                    f.close
                except IOError as e:
                    print "I/O error({0}): {1}".format(e.errno, e.strerror)
                db.commit()    # for older MySQLdb drivers
            except urllib2.HTTPError, e:
                print "could not get '" + link + "' from '" +title +"'"
                print e.code
                print e.read()
                print text


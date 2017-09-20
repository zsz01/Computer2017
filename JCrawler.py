#!/usr/bin/python
#-*-coding:utf-8-*-
# JCrawler
# Author: Jam <810441377@qq.com>
import sys
import cgi
import time
import urllib2
import sqlite3
import thread
from bs4 import BeautifulSoup

# 目标站点
TargetHost = "http://adirectory.blog.com"
# User Agent
UserAgent  = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.117 Safari/537.36'
# 链接采集规则
# 目录链接采集规则
CategoryFind    = [{'findMode':'find','findTag':'div','rule':{'id':'cat-nav'}},
                   {'findMode':'findAll','findTag':'a','rule':{}}]
# 文章链接采集规则
ArticleListFind = [{'findMode':'find','findTag':'div','rule':{'id':'content'}},
                   {'findMode':'findAll','findTag':'h2','rule':{'class':'title'}},
                   {'findMode':'findAll','findTag':'a','rule':{}}]
# 文章内容采集规则
ArticleContentFind    = [{'findMode':'find','findTag':'div','rule':{'id':'content'}}]

# 分页URL规则
PageUrl  = 'page/#page/'
PageStart = 1
PageStep  = 1
PageStopHtml = '404: Page Not Found'

# 数据库初始化
DataBase   = "./Crawler.db"
DataConn   = sqlite3.connect(DataBase,check_same_thread = False)

ThreadMax   = 15 # 最大线程
ThreadLock  = thread.allocate_lock()
ThreadTotal = 0

reload(sys)
sys.setdefaultencoding('utf8')

#print GetCenterText("abc111cde", "abc", "cde")
#return 111
def GetCenterText(text, leftStr, rightStr):
    lPos = len(leftStr) + text.find(leftStr)
    rPos = text.find(rightStr, lPos + len(leftStr))
    return text[lPos:rPos]

def GetHtmlText(url):
    request  = urllib2.Request(url)
    request.add_header('Accept', "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp")
    request.add_header('Accept-Encoding', "*")
    request.add_header('User-Agent', UserAgent)
    return urllib2.urlopen(request).read()

def ArrToStr(varArr):
    returnStr = ""
    for s in varArr:
        returnStr += str(s)
    return returnStr


def GetHtmlFind(htmltext, findRule):
    findReturn = BeautifulSoup(htmltext)
    returnText = ""
    for f in findRule:
        if returnText != "":
            findReturn = BeautifulSoup(returnText)
        if f['findMode'] == 'find':
            findReturn = findReturn.find(f['findTag'], f['rule'])
        if f['findMode'] == 'findAll':
            findReturn = findReturn.findAll(f['findTag'], f['rule'])
        returnText = ArrToStr(findReturn)
    return findReturn

def GetCategory():
    categorys = [];
    htmltext = GetHtmlText(TargetHost)
    findReturn = GetHtmlFind(htmltext, CategoryFind)

    for tag in findReturn:
        print "[G]->Category:" + tag.string + "|Url:" + tag['href']
        categorys.append({'name': tag.string, 'url': tag['href']})
    return categorys;

def GetArticleList(category):
    articles = []
    page = PageStart
    
    while True:
        htmltext = ""
        pageUrl  = PageUrl.replace("#page", str(page))
        print "[G]->PageUrl:" + category['url'] + pageUrl
        while True:
            try:
                htmltext = GetHtmlText(category['url'] + pageUrl)
                break
            except urllib2.HTTPError,e:
                print "[E]->HTTP Error:%s|page:%s" % (e.code, category['url'] + pageUrl)
                if e.code == 404:
                    htmltext = PageStopHtml
                    break
                if e.code == 504:
                    print "[E]->HTTP Error 504: Gateway Time-out, Wait"
                    time.sleep(5)
                else:
                    break

        if htmltext.find(PageStopHtml) >= 0:
            print "End Page."
            break
        else:
            
            findReturn = GetHtmlFind(htmltext, ArticleListFind)

            for tag in findReturn:
                if tag.string != None and tag['href'].find(TargetHost) >= 0:
                    print "[G]->Article:" + tag.string + "|Url:" + tag['href']
                    articles.append({'name': tag.string, 'url': tag['href']})

            page += 1
        
    return articles;

def GetArticle(article):
    htmltext = ""
    while True:
        try:
            htmltext = GetHtmlText(article['url'])
            break
        except urllib2.HTTPError,e:
            print "[E]->HTTP Error:%s|page:%s" % (e.code, article['url'])
            if e.code == 404:
                break
            if e.code == 504:
                print "[E]->HTTP Error 504: Gateway Time-out, Wait"
                time.sleep(5)

    findReturn = GetHtmlFind(htmltext, ArticleContentFind)
    return str(findReturn)



def HtmlEscape(text):
    htmlEscapeText = ""
    htmlEscapeText = cgi.escape(text)
    htmlEscapeText = htmlEscapeText.replace("'", "&apos;")
    htmlEscapeText = htmlEscapeText.replace('"', "&quot;")
    return htmlEscapeText

def DataBaseInit():
    c = DataConn.cursor()
    try:
       c.execute("create table Category(Name varchar(255), Url Text);")
       c.execute("create table Article(Name varchar(255), Category varchar(255), Url Text, Content Text);") 
    except:
        print "[W]->InitDataBase:TABLE is already exist."
    DataConn.commit()
    return;

def DataBaseAddCategorys(categorys):
    c = DataConn.cursor()
    try:
        for category in categorys:
            sql = "insert into Category values('%s','%s');" % (HtmlEscape(category['name']), category['url'])
            c.execute(sql)
    except Exception, e:
        print "[E]->DataBaseAddCategorys:%s" % (str(e))
    DataConn.commit()
    return;

def DataBaseAddArticle(article, category):
    c = DataConn.cursor()
    try:
        sql = "insert into Article values('%s','%s', '%s', '%s');" % (HtmlEscape(article['name']), HtmlEscape(category['name']), article['url'], HtmlEscape(article['content']))
        c.execute(sql)
    except Exception, e:
        print "[E]->DataBaseAddArticle:%s" % (str(e))
    DataConn.commit()
    return;

def Thread_Start(func, argv):
    global ThreadTotal
    while ThreadTotal >= ThreadMax:
        time.sleep(3)

    ThreadLock.acquire()
    ThreadTotal+=1
    ThreadLock.release()
    thread.start_new_thread(func, argv)
    return;


def ThreadMaster():
    print "[G]->GetCategory"
    Mycategorys = GetCategory();
    DataBaseAddCategorys(Mycategorys)
    print "[G]->GetCategory->Success."
    time.sleep(3)
    for category in Mycategorys:
        print "[G]->GetArticleList:" + category['name']
        Thread_Start(ThreadGetArticleList, (category, ThreadTotal))

    while ThreadTotal > 0:
        time.sleep(3)
    
    print "ThreadMaster End."
    return;


def ThreadGetArticleList(category, threadNumber):
    global ThreadTotal
    print "[T]->ThreadGetArticleList->Start: %s, ThreadTotal: %s " % (threadNumber, ThreadTotal)
    articles = GetArticleList(category)
    print "[T]->ThreadGetArticleList->%s,Total:%s" % (category['name'], len(articles))
    Thread_Start(ThreadGetArticle, (articles, category))
    ThreadLock.acquire()
    ThreadTotal -= 1
    ThreadLock.release()
    print "[T]->ThreadGetArticleList->End: %s, ThreadTotal: %s" % (threadNumber, ThreadTotal)
    thread.exit_thread()
    return;

def ThreadGetArticle(articles, category):
    for article in articles:
        article['content'] = ''
        article['content'] = GetArticle(article)
        DataBaseAddArticle(article, category)
    return;



# The Init
DataBaseInit()

ThreadMaster()

# The End
DataConn.close()
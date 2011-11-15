#!/usr/bin/python

import csv,sys,re

ps = [
     (re.compile(r'/(people|artist|users|\w*?tags?|collection|wish|list|doulist|do|badge|doushow|location|host|minisite|cluster|isbn|search|apikey|url)/[^/]+'),r'/\1/-'),
     (re.compile(r'/subject/a[^/]+'),r'/subject/a-'),
     (re.compile(r'/shuo/(?!api|url|accounts|photo|admin|settings|a|[!])[^/]+'),r'/shuo/-'),
     (re.compile(r'/site/(?!censor|cart|invite|about|tos|blank|apply)[^/]+'),r'/site/-'),
     (re.compile(r'/group/(?!mine|discover|local|topic|new_group|all|search|all_topics|topic_search|category|local|my_topics)[^/]+'),r'/group/-'),
     (re.compile(r'=[^&]*'),r'='),
     (re.compile(r'(%\w\w)+'),r'-'),
     (re.compile(r'[\d-]+'),r'-'),
     ]

p_host = re.compile(r"https?://([^/]*)")

def ourl(u):
    if u.startswith("http://") or u.startswith("https://"):
        return p_host.search(u).group(1)
    for p,r in ps:
        u = p.sub(r,u)
    return u

hosts = {
   'www.douban.com': '',
   '9.douban.com': '/ninetaps',
   'm.douban.com': '/m',
   'accounts.douban.com': '/accounts',
   'api.douban.com': '/service/api',
   'book.douban.com': '/book',
   'movie.douban.com': '/movie',
   'music.douban.com': '/music',
   'site.douban.com': '/site',
   'read.douban.com': '/read',
   'alphatown.douban.com': '/town',
   'douban.fm': '/fm',
   'shuo.douban.com': '/shuo',
   'dou.bz': '/shuo/url',
   'alphatown.com': '/town',
   'beijing.douban.com': '/location/beijing',
   'shanghai.douban.com': '/location/shanghai',
   'guangzhou.douban.com': '/location/guangzhou',
}

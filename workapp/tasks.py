#encoding:utf-8
import time
from celery import task
from celery import platforms
from utils import handle_url,getBuckInfoToCache

platforms.C_FORCE_ROOT = True
from django.core.cache import cache
  
@task()
def do_upload(bucket,urlpath):   
    host = 'your-endpoint'
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endPoint = "your-endpoint"
        
    srcheaders_= {}
    if cache.get(bucket) is None:
        getBuckInfoToCache(bucket,access_key,secret_key,endPoint)
        
        cache_headers = cache.get(bucket).get('header') 
        if cache_headers is not None:  
            for elem in cache_headers:
                for key in elem.keys():
                    srcheaders_[key]=elem[key]
        
        cache_keys = cache.get(bucket).get('keys') 
        if cache_keys is not None:
            elem = cache_keys[0]
            if elem is not None:
                akey = elem['access_key']
                skey = elem['secret_key']
                
    else: 
        cache_headers = cache.get(bucket).get('header') 
        if cache_headers is not None:  
            for elem in cache_headers:
                for key in elem.keys():
                    srcheaders_[key]=elem[key]
        
        cache_keys = cache.get(bucket).get('keys') 
        if cache_keys is not None:
            elem = cache_keys[0]
            if elem is not None:
                akey = elem['access_key']
                skey = elem['secret_key']
            
    handle_url(bucket,urlpath,host,akey,skey,srcheaders= srcheaders_)

@task()
def do_work(name):
    for i in range(1,10):
        print 'hello:%s %s' % (name,i)
        time.sleep(1)


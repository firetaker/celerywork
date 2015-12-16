#!/usr/bin/env python
# -*- coding:utf-8 -*-
import boto
from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.connection import SubdomainCallingFormat
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from sys import stderr
import errno
import hashlib
import os
import re
import sys
import tempfile
import traceback
global opts

import hmac, base64, datetime, json, urllib2
import requests
import urlparse 

from django.core.cache import cache

###### Exception classes #######
class ObsyncException(Exception):
    def __init__(self, ty, e):
        if (isinstance(e, str)):
            # from a string
            self.tb = "".join(traceback.format_stack())
            self.comment = e
        else:
            # from another exception
            self.tb = traceback.format_exc(100000)
            self.comment = None
        self.ty = ty


class ObsyncTemporaryException(ObsyncException):
    def __init__(self, e):
        ObsyncException.__init__(self, "temporary", e)


class ObsyncPermanentException(ObsyncException):
    def __init__(self, e):
        ObsyncException.__init__(self, "permanent", e)

class ObsyncArgumentParsingException(ObsyncException):
    def __init__(self, e):
        ObsyncException.__init__(self, "argument_parsing", e)


###### Helper functions #######
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError, exc:
        if exc.errno != errno.EEXIST:
            raise ObsyncTemporaryException(exc)
        if (not os.path.isdir(path)):
            raise ObsyncTemporaryException(exc)
        
def bytes_to_str(b):
    return ''.join(["%02x" % ord(x) for x in b]).strip()

def get_md5(f, block_size=2 ** 20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return "%s" % md5.hexdigest()

def strip_prefix(prefix, s):
    if not (s[0:len(prefix)] == prefix):
        return None
    return s[len(prefix):]

def s3_name_to_local_name(s3_name):
    s3_name = re.sub(r'\$', "$$", s3_name)
    if (s3_name[-1:] == "/"):
        s3_name = s3_name[:-1] + "$slash"
    return s3_name

def local_name_to_s3_name(local_name):
    local_name = re.sub(r'\$slash', "/", local_name)
    mre = re.compile("[$][^$]")
    if mre.match(local_name):
        raise ObsyncPermanentException("Local name contains a dollar sign escape \
sequence we don't understand.")
    local_name = re.sub(r'\$\$', "$", local_name)
    return local_name

###### Object #######
class Object(object):
    def __init__(self, name, md5, size, headers):
        self.name = name
        self.size = int(size)
        self.headers = headers
        
    def set_headers(self, headers):
        self.headers = headers
   

###### LocalCopy ######
class LocalCopy(object):
    def __init__(self, obj_name, path, path_is_temp):
        self.obj_name = obj_name
        self.path = path
        self.path_is_temp = path_is_temp
    def remove(self):
        if ((self.path_is_temp == True) and (self.path != None)):
            os.unlink(self.path)
        self.path = None
        self.path_is_temp = False
    def __del__(self):
        self.remove()
        
###### S3 store #######     
class S3Store():
    def __init__(self, host, bucketname, akey, skey):
        self.host = host
        self.bucket_name = bucketname

        self.conn = S3Connection(
                calling_format=OrdinaryCallingFormat(),
                host=self.host,
                port=9100,
                is_secure=False,
                aws_access_key_id=akey,
                aws_secret_access_key=skey)

        self.bucket = self.conn.lookup(self.bucket_name)
        if (self.bucket == None):            
            raise ObsyncPermanentException("%s: no such bucket as %s" % \
                                                       (host, self.bucket_name))

    def __str__(self):
        return "s3://" + self.host + "/" + self.bucket_name + "/" + self.key_prefix

    def make_local_copy(self, obj):
        k = Key(self.bucket)
        k.key = obj.name
        temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False).name
        try:
            k.get_contents_to_filename(temp_file)
        except Exception, e:
            os.unlink(temp_file)
            raise ObsyncTemporaryException(e)
        return LocalCopy(obj.name, temp_file, True)

    def locate_object(self, obj):
        k = self.bucket.get_key(obj.name)
        if (k == None):
            return None
        return Object(obj.name, None, k.size, None)

    def upload(self, local_copy, obj):
        print "UPLOAD: local_copy.path='" + local_copy.path + "' " + \
                "obj='" + obj.name + "'"
        k = Key(self.bucket)
        k.key = obj.name
        k.set_contents_from_filename(local_copy.path, obj.headers)
        # k.set_contents_from_filename(local_copy.path)
        k.set_canned_acl('public-read', None)
        
    def remove(self, obj):
        self.bucket.delete_key(obj.name)
        if (opts.more_verbose):
            print "S3Store: removed %s" % obj.name
            


######## Http storage ###########
def http_meta_to_headers(rest_headers):
    headers = {}
    if rest_headers is not None:
        if rest_headers['cache-control'] is not None:
            headers['Cache-Control'] = rest_headers['cache-control']
        if rest_headers['content-type'] is not None:
            headers['Content-Type'] = rest_headers['content-type']
    return headers
 
    
class HttpUrlStore():
    def __init__(self, bucketname, urlpath, srcheaders):
        self.srcheaders = srcheaders
        self.bucket_name = bucketname,
        self.urlpath = urlpath
    
    def get_object(self):
        obj_name = urlparse.urlparse(self.urlpath).path
        return Object(urllib2.url2pathname(obj_name), None, 0, None)
    
    def make_local_copy(self, obj):
        # req_headers={'Host':'abc.com'};
        result = requests.get(self.urlpath, headers=self.srcheaders, timeout=180, allow_redirects=False)
         
        if not result.ok:
            return None
        obj.headers = http_meta_to_headers(result.headers)
        
        temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False).name
        try:
            with open(temp_file, 'wb') as download_file:
                download_file.write(result.content)  # TODO big file process
        except Exception, e:
            os.unlink(temp_file)
            raise ObsyncTemporaryException(e)  
        return LocalCopy(obj.name, temp_file, True)
 
############Begin#############
def handle_url(bucket_name, urlpath, host, akey, skey, srcheaders):
    print(bucket_name)
    print(urlpath)
    try:
        src = HttpUrlStore(bucket_name, urlpath, srcheaders)
    except ObsyncException, e:
        raise
    try:
        dst = S3Store(host, bucket_name, akey, skey)
    except ObsyncException, e:
        raise

    try:
        sobj = src.get_object()
    except StopIteration:
        exit(1)
    
    upload = True      
    if dst.locate_object(sobj) is not None:
        upload = False    
         
    print "handling " + sobj.name 
    if (upload):
        local_copy = src.make_local_copy(sobj)
        if local_copy is None:
            exit(1)
        try:
            dst.upload(local_copy, sobj)
        finally:
            local_copy.remove()
            
def authstr(method, urlpath, datestr, ak, sk): 
    mystr = method + "\n\n\n" + datestr + "\n" + urlpath
    myhmac = hmac.new(sk, digestmod=hashlib.sha1) 
    myhmac.update(mystr) 
    signstr = base64.encodestring(myhmac.digest()) 
    result = "AWS %s:%s" % (ak, signstr) 
    return result.strip() 

def getbuckmap(bucket_name, access_key, secret_key, endPoint):
    GMTFORMAT = '%a, %d %b %Y %H:%M:%S GMT'
    
    urlpath = '%s/admin/bucket?map_url&format=json&bucket=%s' % (endPoint, bucket_name)
    headers = {}
    mydate = datetime.datetime.utcnow().strftime(GMTFORMAT)
  
    headers['Date'] = mydate 
    headers['Authorization'] = authstr("GET", "/admin/bucket", mydate, access_key, secret_key)
    result = requests.get(urlpath, headers=headers, timeout=120)
    
    if not result.ok:
            return None
    # return json.dumps(result.content)
    return json.loads(result.content)
    
def getUserIdByBucket(bucket_name, access_key, secret_key, endPoint):
    GMTFORMAT = '%a, %d %b %Y %H:%M:%S GMT'
    urlpath = '%s/admin/bucket?bucket=%s&stats=False' % (endPoint, bucket_name)
    headers = {}
    mydate = datetime.datetime.utcnow().strftime(GMTFORMAT)
  
    headers['Date'] = mydate 
    headers['Authorization'] = authstr("GET", "/admin/bucket", mydate, access_key, secret_key)
    result = requests.get(urlpath, headers=headers, timeout=120)
    
    if not result.ok:
            return None
    # return json.dumps(result.content)
    return json.loads(result.content)['owner']

def getKeysByUid(ownerid, access_key, secret_key, endPoint):
    GMTFORMAT = '%a, %d %b %Y %H:%M:%S GMT'
    urlpath = '%s/admin/user?uid=%s' % (endPoint, ownerid)
    headers = {}
    mydate = datetime.datetime.utcnow().strftime(GMTFORMAT)
  
    headers['Date'] = mydate 
    headers['Authorization'] = authstr("GET", "/admin/user", mydate, access_key, secret_key)
    result = requests.get(urlpath, headers=headers, timeout=120)
    
    if not result.ok:
            return None
    # return json.dumps(result.content)
    return json.loads(result.content)

def getBuckInfoToCache(bucket_name, access_key, secret_key, endPoint):
    bucket_user_info = {}
    result = getbuckmap(bucket_name, access_key, secret_key, endPoint)
    if result is not None:
        bucket_user_info['header'] = result.get('header')
        # cache.set('img6n.soufunimg.com',result.get('header'))    
    ownid = getUserIdByBucket(bucket_name, access_key, secret_key, endPoint)
    if ownid is not None:
        ret = getKeysByUid(ownid, access_key, secret_key, endPoint)
        # cache.set('img6n.soufunimg.com',ret['keys'])
        bucket_user_info['keys'] = ret['keys']
    
    cache.set(bucket_name, bucket_user_info)

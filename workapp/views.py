# encoding:utf-8
from django.shortcuts import render
from django.http import HttpResponse
from django.views.generic import View
from django.core.cache import cache

# Create your views here.

from workapp import utils

from workapp.tasks import do_work

class Hello(View):
    def get(self, request, *args, **kwargs):
        access_key = "test-access_key"
        secret_key = "test-secret_key"
        endPoint = "test-end-point"
        utils.getBuckInfoToCache('img6n.soufunimg.com',access_key,secret_key,endPoint)
        
        return HttpResponse('Hello, World! %s' % (cache.get('your-bucket-name')))
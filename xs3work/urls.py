from django.conf.urls import patterns, include, url
from django.contrib import admin

from workapp.views import Hello
from restapp import views

urlpatterns = patterns('',
    url(r'^tees$',Hello.as_view()),
    url(r'^api/', views.BucketUrlList.as_view()),
    url(r'^admin/', include(admin.site.urls)),
)

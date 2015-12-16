from django.db import models

# Create your models here.
class BucketUrl(models.Model):
    bucket = models.CharField(max_length=1024)
    urlpath = models.URLField(max_length=2048)
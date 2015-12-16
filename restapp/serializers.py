from .models import BucketUrl
from rest_framework import serializers

class RestReqSerializer(serializers.ModelSerializer):
    class Meta:
        model = BucketUrl
        fields = ('bucket', 'urlpath')
from .models import BucketUrl
from django.http import Http404

from restapp.serializers import RestReqSerializer
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import djcelery
from workapp import tasks


class BucketUrlList(APIView):
    def get(self, request, format=None):
        users = BucketUrl.objects.all()
        serializer = RestReqSerializer(users, many=True)
        return Response(serializer.data)
   
    def on_result(self, response):
        print 'result:'. response.result
        #self.write(str(response.result))
        #self.finish()
        
    def post(self, request, format=None):
        serializer = RestReqSerializer(data=request.DATA)
        if serializer.is_valid():
            #serializer.save()
            print(serializer.data.get("bucket"))
            print(serializer.data.get("urlpath"))
            tasks.do_upload.apply_async(args=[serializer.data.get("bucket"),serializer.data.get("urlpath")],
                                        callback=self.on_result)
            return Response("", status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

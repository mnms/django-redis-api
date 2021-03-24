from django.shortcuts import render

# Create your views here.

import json
from django.conf import settings
import redis
from rest_framework.decorators import api_view
from rest_framework import status
from rest_framework.response import Response

import os
import time
from django.http.response import JsonResponse
from rest_framework.parsers import JSONParser

# Connect to our Redis instance
redis_instance = redis.StrictRedis(host=settings.REDIS_HOST,
                                  port=settings.REDIS_PORT, db=0)

spark_submit_cmd_new = """
                export LIGHTNINGDB_LIB_PATH=$HOME/tsr2/cluster_1/tsr2-assembly-1.0.0-SNAPSHOT/lib/; 
                export COMMON_CLASSPATH=$(find $LIGHTNINGDB_LIB_PATH -name 'tsr2*' -o -name 'spark-r2*' -o -name '*jedis*' -o -name 'commons*' -o -name 'jdeferred*' -o -name 'geospark*' -o -name 'gt-*' | tr '\n' ':'); 
                export EXECUTOR_CLASSPATH=${COMMON_CLASSPATH%:}; 
                export EXTRA_CLASSPATH=$(echo $EXECUTOR_CLASSPATH | tr ':' '\n' | awk -F '/' '{print $NF}' | tr '\n' ':' | sed 's/:$//'); 
                export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop; 
                """ + " $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --name Ltdb-PCA --num-executors 3 --driver-memory 20g --executor-memory 4g --executor-cores 10 --conf spark.ui.port=4047 --driver-class-path $EXECUTOR_CLASSPATH  --jars ${EXECUTOR_CLASSPATH//:/,} --conf spark.executor.extraClassPath=$EXTRA_CLASSPATH  --class com.ltdb.QueryApp /home/svcapp_su/utils/ltdbPca/LtdbPca-ltdbpca-1.3-SNAPSHOT.jar "

drop_table_cmd_new = "DROP TABLE IF EXISTS instances2"
create_table_cmd_new = """CREATE TABLE instances2 (instance_id long, project_id long,
                dataset_id long, file_id long, cropped_image string,
                track_id long, binding_id long, class_name string,
                region_type string, region string, create_date string,
                update_date string, feature_value array<float>,
                feature_value_3d string, model_id long, extracting_method string,
                gender string, age string, expression string, action string,
                posture string, color string, cropped_image_path string,
                file_id_key long, model_id_is_not_null boolean, tags string)
                USING r2 OPTIONS (table '601', host 'bpp-tview-aiops-search01',
                port '18600',
                partitions 'project_id model_id_is_not_null file_id_key',
                mode 'nvkvs', rowstore 'false', at_least_one_partition_enabled 'no')
                """

from django.db import models


class Instances(models.Model):
    query = models.CharField(max_length=200,blank=False, default='')

from rest_framework import serializers
class JsonSerializer(serializers.ModelSerializer):

    class Meta:
        model = Instances
        fields = ('query')

@api_view(['GET', 'POST'])
def manage_items(request, *args, **kwargs):
    if request.method == 'GET':
        items = {}
        count = 0
        for key in redis_instance.keys("*"):
            items[key.decode("utf-8")] = redis_instance.get(key)
            count += 1
        response = {
            'count': count,
            'msg': f"Found {count} items.",
            'items': items
        }
        return Response(response, status=200)
    elif request.method == 'POST':
        item = json.loads(request.body)
        key = list(item.keys())[0]
        value = item[key]
        redis_instance.set(key, value)
        response = {
            'msg': f"{key} successfully set to {value}"
        }
        return Response(response, 201)

@api_view(['GET', 'PUT','POST', 'DELETE'])
def manage_item(request, *args, **kwargs):
    if request.method == 'GET':
        if kwargs['key']:
            value = redis_instance.incr(kwargs['key'])
            if value:
                response = {
                    'key': kwargs['key'],
                    'value': value,
                    'msg': 'success'
                }
                return Response(response, status=200)
            else:
                response = {
                    'key': kwargs['key'],
                    'value': None,
                    'msg': 'Not found'
                }
                return Response(response, status=404)
    elif request.method == 'PUT':
        if kwargs['key']:
            request_data = json.loads(request.body)
            new_value = request_data['new_value']
            value = redis_instance.get(kwargs['key'])
            if value:
                redis_instance.set(kwargs['key'], new_value)
                response = {
                    'key': kwargs['key'],
                    'value': value,
                    'msg': f"Successfully updated {kwargs['key']}"
                }
                return Response(response, status=200)
            else:
                response = {
                    'key': kwargs['key'],
                    'value': None,
                    'msg': 'Not found'
                }
                return Response(response, status=404)
    elif request.method == 'POST':
        if kwargs['key'] == 'create':
            cmd =   spark_submit_cmd_new + '"' + create_table_cmd_new + '"'
            result = os.popen(cmd).read()
            response = {
                'cmd': {cmd},
                'result': {result}
            }
            return Response(response, status=200)
        elif kwargs['key'] == 'drop':
            cmd =   spark_submit_cmd_new + '"' + drop_table_cmd_new + '"'
            result = os.popen(cmd).read()
            response = {
                'cmd': {cmd},
                'result': {result}
            }
            return Response(response, status=200)
        elif kwargs['key'] == 'query':
            start = time.time()
            parsed_request = JSONParser().parse(request)
            sqlquery = parsed_request['query']
            cmd =   spark_submit_cmd_new + '"' + sqlquery + '"'
            result = os.popen(cmd).read()
            elapsed_time = time.time() - start
            response = {
                'elapsed_time': {elapsed_time},
                'cmd': {sqlquery},
                'result': {result},
                'elapsed_time': {elapsed_time}
            }
            return Response(response, status=200)
        elif kwargs['key'] == 'pca':
            start = time.time()
            parsed_request = JSONParser().parse(request)
            sqlquery = parsed_request['query']

            cmd =   spark_submit_cmd_new + " 'PCA: " + sqlquery + "'"
            result = os.popen(cmd).read()
            elapsed_time = time.time() - start
            response = {
                'elapsed_time': {elapsed_time},
                'query': {sqlquery},
                'result': {result},
                'elapsed_time': {elapsed_time}
            }
            return Response(response, status=200)
        else:
            response = {
                'key': kwargs['key'],
                'msg': 'Not found'
            }
            return Response(response, status=404)
    elif request.method == 'DELETE':
        if kwargs['key']:
            result = redis_instance.delete(kwargs['key'])
            if result == 1:
                response = {
                    'msg': f"{kwargs['key']} successfully deleted"
                }
                return Response(response, status=404)
            else:
                response = {
                    'key': kwargs['key'],
                    'value': None,
                    'msg': 'Not found'
                }
                return Response(response, status=404)

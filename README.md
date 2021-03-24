# 목적

REST API 기반 질의 및 PCA 연산을 위해 개발 및 환경을 구축함

# 최초 실행 시
$ virtualenv --python=python3 env --no-site-packages
$ source env/bin/activate
$ pip install django djangorestframework redis


# 재시작 시
$ scl enable rh-python36 bash

$ python -V
Python 3.6.12

(env) $ pwd
/home/svcapp_su/utils/redis-api/django_redis_api

(env) $ nohup python manage.py runserver 0.0.0.0:8765  > django.log &


# APIs to allocate a new _id with using Redis Commands

## Run redis-server
```
$ redis-cli
127.0.0.1:6379> keys *
1) "instance_id"
2) "model_id"
127.0.0.1:6379> get instance_id
"4"
127.0.0.1:6379> incr instance_id
(integer) 5
127.0.0.1:6379> incr instance_id
(integer) 6
127.0.0.1:6379>
```

## 새로운 id 가져오는 방법(incr command 사용)
```
URL: curl --location --request GET http://10.40.88.92:8765/api/{id name}
```

사용 예,
```
$ curl --location --request GET http://10.40.88.92:8765/api/model_id
{"key":"model_id","value":1,"msg":"success"}
$ curl --location --request GET http://10.40.88.92:8765/api/model_id
{"key":"model_id","value":2,"msg":"success"}
$ curl --location --request GET http://10.40.88.92:8765/api/model_id
{"key":"model_id","value":3,"msg":"success"}
$ curl --location --request GET http://10.40.88.92:8765/api/instance_id
{"key":"instance_id","value":2,"msg":"success"}
$ curl --location --request GET http://10.40.88.92:8765/api/instance_id
{"key":"instance_id","value":3,"msg":"success"}
```

## id값 리셋하는 방법
```
URL: curl --location --request PUT http://10.40.88.92:8765/api/instance_id --data '{"new_value": "/type/new/value"}'
```

사용 예,
```
$ curl --location --request PUT http://10.40.88.92:8765/api/instance_id --data '{"new_value": "10000"}'
{"key":"instance_id","value":"1","msg":"Successfully updated instance_id"}
$ curl --location --request GET http://10.40.88.92:8765/api/instance_id
{"key":"instance_id","value":10001,"msg":"success"}
```

# APIs for PCA

## 테이블 생성

```
curl --location --request POST http://10.40.88.92:8765/api/create --header "Content-Type: text/plain" --data 'create'
```

## 테이블 제거
```
curl --location --request POST http://10.40.88.92:8765/api/drop --header "Content-Type: text/plain" --data 'drop' 
```

## Query
```
[svcapp_su@BPP-TVIEW-AIOPS-SEARCH01 ~]$ curl --location --request POST http://10.40.88.92:8765/api/query --header "Content-Type: text/plain" --data '{"query":"SELECT project_id, instance_id, feature_value, cropped_image_path FROM instances2 WHERE model_id = 1 AND (project_id = 5 OR project_id = 108)"}'


{"elapsed_time":[22.071696043014526],"cmd":["SELECT project_id, instance_id, feature_value, cropped_image_path FROM instances2 WHERE model_id = 1 AND (project_id = 5 OR project_id = 108)"],"result":["[{\"project_id\":108,\"instance_id\":7553625,\"feature_value\":[0.35542652,0.5420146,0.42736512,0.38785133,0.48206323,0.48518333,0.43490148,0.5042965,0.49428207,0.37250596,0.28475612,0.35937348,0.48797792,0.50746137,0.39251664,0.46511638,0.5962325,0.2686054,0.42894325,0.42551893,0.3258215,0.51388115,0.44065857,0.41618633,0.5295157,0.51960707,0.4763249,0.39503786,0.44422296,0.3771433,0.41929758,0.45370558,0.37161332,0.49084604,0.65738505,0.4750429,0.44083253,0.42128822,0.4113718,0.5934207,0.4469131,0.49588057,0.38446122,0.35168803,0.3548299,0.5160891,0.38118386,0.518747,0.44242555,0.54260063,0.44359797,0.3967149,0.52373546,0.44709077,0.537754,0.47580478,0.39291868,
```

## PCA

where 절을 수정해서 사용('model_id_is_not_null = true'가 반드시 들어가야 함)
select에는 'feature_value'가 반드시 있어야 함

```
curl --location --request POST http://10.40.88.92:8765/api/pca --header "Content-Type: text/plain" --data '{"query":"SELECT project_id, instance_id, feature_value, cropped_image_path FROM instances2 WHERE model_id = 1 AND (project_id = 5 OR project_id = 108)"}'


{"elapsed_time":[62.88131785392761],"query":["SELECT project_id, instance_id, feature_value, cropped_image_path FROM instances2 WHERE model_id = 1 AND (project_id = 5 OR project_id = 108)"],"result":["[{\"project_id\":5,\"instance_id\":18987,\"feature_value_3d\":{\"type\":1,\"values\":[10.01856613190646,-5.511636906776,0.6778439156438683]},\"cropped_image_path\":\"/metavision/vol1/MEDIA/IMAGE_DATA/crop/18987_120948_00_00085.jpg\"}, {\"project_id\":5,\"instance_id\":18112,\"feature_value_3d\":{\"type\":1,\"values\":[4.304595613722197,6.863886710889437,-0.5475611387657388]},\"cropped_image_path\":\"/metavision/vol1/MEDIA/IMAGE_DATA/crop/18112_111004_00_00339.jpg\"}, {\"project_id\":5,\"instance_id\":17578,\"feature_value_3d\":{\"type\":1,\"values\":[-6.268511425783814,8.276896556369845,-4.586932261524279]},\"cropped_image_path\":\"/metavision/vol1/MEDIA/IMAGE_DATA/crop/17578_105421_01_00127.jpg\"}, ...

```



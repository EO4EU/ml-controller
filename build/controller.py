from flask import Flask, request, make_response
import uuid
import json
import kubernetes
from kubernetes import client, config
import sys
import logging

from KafkaHandler import KafkaHandler,DefaultContextFilter

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves
from kafka import KafkaProducer

def convert_bytes(obj):
    if isinstance(obj, dict):
        return {key: convert_bytes(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_bytes(item) for item in obj]
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')  # Decode bytes to string
    else:
        return obj

def create_app():
    app = Flask(__name__)
    Producer=KafkaProducer(bootstrap_servers="kafka-external.dev.apps.eo4eu.eu:9092",value_serializer=lambda v: json.dumps(v,default=convert_bytes).encode('utf-8'),key_serializer=str.encode)
    handler = KafkaHandler(producer=Producer,source='ML.Controller')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    filter = DefaultContextFilter()
    app.logger.addFilter(filter)
    app.logger.addHandler(handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.DEBUG)

    app.logger.info("Starting up...", extra={'logName': 'startup'})

    # Provisioner receiving topics to listen by http in a post request.
    
    # Main entry point of the http request.
    # This function is called when a post request is received on the root path.
    # The post request must contain a json with the following format:
    # {
    #   "Topics": {
    #     "in": "topic_in",
    #     "out": "topic_out"
    #   }
    #   "ML": {
    #     "ServiceName": "service_name",
    #     "Namespace": "namespace"
    #   }
    # }
    # The function will create a KafkaSource in the namespace namespace.
    # The KafkaSource will listen to the topic topic_in and send the message to the service service_name in the namespace namespace.
    # The function will also create a ConfigMap in the namespace namespace.
    # The ConfigMap will contain the json of the request.
    # The ConfigMap will be used by the service service_name to get the json of the request.
    #
    # The post request need to additionally give information for the service like s3 bucket information and model parameters.
    #
    @app.route('/', methods=['POST'])
    def controller_post():
        # TODO : Debugging message to remove in production.
        # Message received.
        app.logger.info(request.data, extra={'logName': 'request.data'})
        response=None
        # Try block to ensure the service answer even if the message is malformed.
        try:
            # Load the json in the message and extract topic information.
            data=json.loads(request.data)
            status=data["workflow_status"]
            if status == "stopping":
                name = data["workflow_name"]
                topic_in=data["Topics"]["in"]
                namespace=data["ML"]["Namespace"]
                deleteResources(namespace,name,topic_in)
            elif status == "published":
                topic_in=data["Topics"]["in"]
                topic_out=data["Topics"][ "out"]
                serviceName=data["ML"]["ServiceName"]
                namespace=data["ML"]["Namespace"]
                # TODO : Debugging message to remove in production.
                app.logger.info('topic _in '+topic_in, extra={'logName': 'topic_in'})
                app.logger.info('topic _out '+topic_out, extra={'logName': 'topic_out'})
                app.logger.info('serviceName '+serviceName, extra={'logName': 'serviceName'})
                app.logger.info('namespace '+namespace, extra={'logName': 'namespace'})
                createResources(topic_in,namespace,serviceName,request.data)

            # HTTP answer that the message is received and valid. This message will then be discarded only the fact that a sucess return code is returned is important.
            response = make_response({
            "msg": "Received message And valid"
            })
        except Exception as e: 
            app.logger.error('Got exception '+str(e), extra={'logName': 'exception'})
            app.logger.info('So we are ignoring the message',extra={'logName': 'ignore'})
            # HTTP answer that the message is malformed. This message will then be discarded only the fact that a sucess return code is returned is important.
            response = make_response({
            "msg": "There was a problem ignoring"
            })
        return response

    def truncate_string(string):
        string=string.replace("-","")
        string=string.replace(".","")
        if len(string) > 60:
            return string[:60]
        else:
            return string
        
    def deleteResources(namespace,name,topic_in):
        config.load_incluster_config()
        api = kubernetes.client.CustomObjectsApi()
        try:
            api.delete_namespaced_custom_object(
                group="sources.knative.dev",
                version="v1beta1",
                plural="kafkasources",
                namespace=namespace,
                name=truncate_string(topic_in.lower()))
            app.logger.info('KafkaSource deleted '+truncate_string(topic_in.lower()), extra={'logName': 'KafkaSourceDeleted'})
            api_instance = client.CoreV1Api()
            bodyDelete = client.V1DeleteOptions()
            api_instance.delete_namespaced_config_map("json-config-"+topic_in.lower(),namespace, body=bodyDelete)
            app.logger.info('ConfigMap deleted '+'json-config-'+topic_in.lower(), extra={'logName': 'ConfigMapDeleted'})
        except Exception as e:
            app.logger.error('Got exception '+str(e), extra={'logName': 'exception'})

    # This function create the needed KafkaSource in the cluster.
    # The KafkaSource will listen to the topic TopicIn and send the message to the service serviceName in the namespace namespace.
    # The function will also create a ConfigMap in the namespace namespace.
    # The ConfigMap will contain the json of the request.
    # The ConfigMap will be used by the service serviceName to get the json of the request.
    #
    def createResources(TopicIn,namespace,serviceName,requestData):
        config.load_incluster_config()
        api = kubernetes.client.CustomObjectsApi()
        try:
            resourceBody={
                "apiVersion": "sources.knative.dev/v1beta1",
                "kind": "KafkaSource",
                "metadata": {
                "name": truncate_string(TopicIn.lower()),
                "namespace": namespace
            },
                "spec": {
                    "initialOffset": "latest",
                    "bootstrapServers": ["kafka-external.dev.apps.eo4eu.eu:9092"],
                    "topics": [TopicIn],
                    "sink": {
                        "ref": {
                            "apiVersion": "serving.knative.dev/v1",
                            "kind": "Service",
                            "name": serviceName.lower(),
                            "namespace": namespace,
                        },
                        "uri": "/json-config-"+TopicIn.lower()
                    }
                }
            }

            config_map = client.V1ConfigMap()
            config_map.metadata = client.V1ObjectMeta(name="json-config-"+TopicIn.lower(),namespace=namespace)

            config_map.data={
                "jsonSuperviserRequest": requestData.decode(),
                "bootstrapServers": "kafka-external.dev.apps.eo4eu.eu:9092",
            }

            api_instance = client.CoreV1Api()
            api_response = api_instance.create_namespaced_config_map(namespace, config_map)

            resource = api.create_namespaced_custom_object(
                group="sources.knative.dev",
                version="v1beta1",
                plural="kafkasources",
                namespace=namespace,
                body=resourceBody)
            app.logger.info('Resources created', extra={'logName': 'ResourcesCreated'})
        except Exception as e:
            app.logger.error('Got exception '+str(e), extra={'logName': 'exception'})

    return app

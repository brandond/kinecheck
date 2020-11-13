#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# This is a terrible ugly hack of a script.

import random
import string
from time import sleep
from kubernetes import client, config
from kubernetes.client import Configuration

namespace = "load-test"
configmaps = dict()

config.load_kube_config()
print(f"Connecting to {Configuration.get_default_copy().host}")

v1 = client.CoreV1Api()


def main():
    ns = client.V1Namespace(
            metadata=client.V1ObjectMeta(
                name=namespace))

    try:
        v1.create_namespace(ns)
    except client.exceptions.ApiException as e:
        if e.status == 409:
            pass
        else:
            raise e

    create_configmaps()
    update_configmaps()
    while True:
        check_configmaps()
        sleep(30)


def refresh_configmap(cm):
    try:
        print(f"Refreshing {cm.metadata.name} rev={cm.metadata.resource_version}")
        return v1.read_namespaced_config_map(name=cm.metadata.name, namespace=namespace)
    except client.exceptions.ApiException as e:
        print(f"\tError: {e.status}")


def update_or_merge_configmap(cm):
    while True:
        try:
            print(f"Updating {cm.metadata.name} rev={cm.metadata.resource_version}")
            return v1.replace_namespaced_config_map(name=cm.metadata.name, namespace=namespace, body=cm)
        except client.exceptions.ApiException as e:
            print(f"\tError: {e.status}")
            if e.status == 409 and 'StorageError: invalid object' in e.body:
                print("\tCreating and merging...")
                cm1 = create_or_get_configmap(cm)
                if not cm1.data:
                    cm1.data = dict()
                cm1.data.update(cm.data)
                cm = cm1
            elif e.status == 409 and 'the object has been modified' in e.body:
                print("\tReading and merging...")
                try:
                    cm1 = v1.read_namespaced_config_map(name=cm.metadata.name, namespace=namespace)
                    if not cm1.data:
                        cm1.data = dict()
                    cm1.data.update(cm.data)
                    cm = cm1
                except client.exceptions.ApiException as e1:
                    print(f"\t\tError: {e1.status}")
            else:
                print(e)


def create_or_get_configmap(cm):
    cm.metadata.resource_version = ''
    while True:
        try:
            print(f"Creating {cm.metadata.name}")
            return v1.create_namespaced_config_map(namespace=namespace, body=cm)
        except client.exceptions.ApiException as e:
            print(f"\tError: {e.status}")
            if e.status == 409:
                print("\tReading existing...")
                try:
                    return v1.read_namespaced_config_map(name=cm.metadata.name, namespace=namespace)
                except client.exceptions.ApiException as e1:
                    if e1.status == 404:
                        pass
                    else:
                        raise e1
            else:
                raise e


def try_delete_configmap(cm):
    try:
        print(f"Deleting {cm.metadata.name} rev={cm.metadata.resource_version}")
        options = client.V1DeleteOptions(
                    preconditions=client.V1Preconditions(
                        resource_version=cm.metadata.resource_version))
        v1.delete_namespaced_config_map(name=cm.metadata.name, namespace=namespace, body=options)
        return True
    except client.exceptions.ApiException as e:
        print(f"\tError: {e.status}")
        if e.status == 404:
            return True
        elif e.status == 409:
            return False
        else:
            raise e


def create_configmaps():
    for i in range(0, 2000):
        cm = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(
                    name=f"test-{i:04}"))
        configmaps[i] = create_or_get_configmap(cm)


def update_configmaps():
    for i, cm in configmaps.items():
        j = random.randint(0, 99)
        k = random.randint(0, 512)
        if not cm.data:
            cm.data = dict()
        cm.data[f"key-{j:02}"] = ''.join(random.choices(string.printable, k=k))
        configmaps[i] = update_or_merge_configmap(cm)


def check_configmaps():
    error = False
    for i, cm in configmaps.items():
        cm = refresh_configmap(cm)
        if cm:
            configmaps[i] = cm
        else:
            error = True
    if error:
        raise Exception("Failed to refresh one or more ConfigMaps!")


def delete_configmaps():
    for i, cm in configmaps.items():
        try_delete_configmap(cm)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass

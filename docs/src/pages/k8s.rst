Kubernetes (k8s)
****************

Overview
========

Deploying SnowAlert in k8s is relatively straightforwared.
It consists of a k8s Deployment type that is for the SnowAlert UI,
and an associated service with the type LoadBalancer. Additionally
there is a CronJob type that runs the SnowAlert runner. The
secrets are all managed via k8s.

Overview
========
Using the included manifests is a good start to get up and running,
however you will want to adjust namespaces and create roles that are
suitable for your environment.

Quickstart
==========
To use the included manifests (warninig, they will deploy into the default
namespace). Modify the sa-secrets.yaml file with base64 encoded values that are
the result of the non-KMS install process (i.e. the values contained within the
envs file).

To Deploy
=========
Assuming you have kubectl configured and pointing to your cluster you can execute

.. code::

    $ kubectl apply -f manifests 

from within the /infra/k8s/ directory.


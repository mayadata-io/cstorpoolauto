## High Level Deployment
CStor Pool Auto controller will consist of following deployments:

### Core Deployment
Core deployment implies the actual business logic. It will consist of following components:
- cspauto (a http service)
- cspauto-controller (a http client & kubernetes controller)

#### cspauto
cspauto implements business logic to auto provision cstor pool(s). This is deployed as a http service that exposes one or more http endpoints. These endpoints get invoked by cspauto controller.

#### cspauto-controller
cspauto controller watches Kubernetes resource(s) and invokes appropriate http endpoints exposed by cspauto service. In other words, this is the http client for cspauto service.

### Conformance Deployment (optional)
Conformance deployment has the conformance logic to verify if cspauto service is working as expected. This is completely optional and can be installed or un-installed without any disruptions to the core services. Conformance deployment will consist of following components:
- cspauto-conformance (a conformance http service)
- cspauto-conformance-controller (a http client & kubernetes controller)

#### cspauto-conformance
cspauto conformance implements conformance logic to verify if cspauto service is functioning properly. This is deployed as a http service. This exposes one or more http endpoints that get invoked by cspauto conformance controller.

#### cspauto-conformance-controller
cspauto conformance controller watches Kubernetes resource(s) and invokes appropriate http endpoints exposed by cspauto conformance service. In other words, this is the http client for cspauto conformance service.